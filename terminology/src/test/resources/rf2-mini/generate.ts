/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Generates the "rf2-mini" synthetic SNOMED CT RF2 snapshot fixtures used by the
// local terminology tests. The output is deterministic: re-running this script
// with the same inputs produces byte-identical files, so the well-known codes
// documented in README.md remain stable.
//
// Run with: bun run terminology/src/test/resources/rf2-mini/generate.ts
//
// Author: John Grimes

import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

// --- Verhoeff check-digit (SNOMED CT dihedral group D5). ---

const DIHEDRAL: number[][] = [
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
  [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
  [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
  [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
  [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
  [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
  [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
  [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
  [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
];

const FNF: number[][] = (() => {
  const table: number[][] = Array.from({ length: 8 }, () =>
    new Array(10).fill(0),
  );
  table[0] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
  table[1] = [1, 5, 7, 6, 2, 8, 3, 0, 9, 4];
  for (let i = 2; i < 8; i++) {
    for (let j = 0; j < 10; j++) {
      table[i][j] = table[i - 1][table[1][j]];
    }
  }
  return table;
})();

const INVERSE_D5 = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9];

/** Appends a Verhoeff check-digit to the partial identifier, returning a full SCTID. */
function withCheckDigit(partial: string): string {
  let check = 0;
  const length = partial.length;
  for (let i = length - 1; i >= 0; i--) {
    const pos = length - i;
    const digit = partial.charCodeAt(i) - 48;
    check = DIHEDRAL[check][FNF[pos % 8][digit]];
  }
  return partial + String(INVERSE_D5[check]);
}

// --- Identifier allocation. ---

let conceptItem = 1000;
let descriptionItem = 5000;
let relationshipItem = 80000;
let refsetSequence = 0;

/** Allocates the next synthetic concept SCTID (short format, concept partition 00). */
function nextConceptId(): string {
  return withCheckDigit(String(conceptItem++) + "00");
}

/** Allocates the next synthetic description SCTID (short format, description partition 01). */
function nextDescriptionId(): string {
  return withCheckDigit(String(descriptionItem++) + "01");
}

/** Allocates the next synthetic relationship SCTID (short format, relationship partition 02). */
function nextRelationshipId(): string {
  return withCheckDigit(String(relationshipItem++) + "02");
}

/** Allocates the next deterministic reference set member UUID. */
function nextRefsetUuid(): string {
  const n = (refsetSequence++).toString(16).padStart(12, "0");
  return `00000000-0000-4000-8000-${n}`;
}

// --- SNOMED CT metadata identifiers (real International SCTIDs). ---

const ROOT = "138875005"; // SNOMED CT Concept.
const CORE_MODULE = "900000000000207008"; // SNOMED CT core module.
const DEFINED = "900000000000073002"; // Sufficiently defined by necessary conditions.
const PRIMITIVE = "900000000000074008"; // Not sufficiently defined (primitive).
const FSN = "900000000000003001"; // Fully specified name.
const SYNONYM = "900000000000013009"; // Synonym.
const US_ENGLISH_REFSET = "900000000000509007"; // US English language reference set.
const GB_ENGLISH_REFSET = "900000000000508004"; // GB English language reference set.
const PREFERRED = "900000000000548007"; // Preferred.
const ACCEPTABLE = "900000000000549004"; // Acceptable.
const IS_A = "116680003"; // Is a (attribute).
const FINDING_SITE = "363698007"; // Finding site (attribute).
const ASSOCIATED_MORPHOLOGY = "116676008"; // Associated morphology (attribute).
const SAME_AS_REFSET = "900000000000527005"; // SAME AS association reference set.

const EFFECTIVE_TIME_V1 = "20230601";
const EFFECTIVE_TIME_V2 = "20240601";

/**
 * A language reference set of a release. `divergent` marks a reference set that prefers a concept's
 * dialect variant, where the concept declares one, rather than its default preferred term.
 */
interface LanguageRefset {
  id: string;
  divergent: boolean;
}

/** The language reference sets of the two International releases: US English, then GB English. */
const INTERNATIONAL_REFSETS: LanguageRefset[] = [
  { id: US_ENGLISH_REFSET, divergent: false },
  { id: GB_ENGLISH_REFSET, divergent: true },
];

// --- In-memory model. ---

interface Concept {
  id: string;
  active: boolean;
  defined: boolean;
  fsn: string;
  /** The preferred synonym; also the default display. */
  preferredTerm: string;
  /** Additional acceptable synonyms. */
  acceptableSynonyms: string[];
  /**
   * A synonym that a divergent language reference set prefers instead of `preferredTerm`, which that
   * reference set then marks merely acceptable. Absent for a concept the reference sets agree about.
   */
  divergentTerm?: string;
  parents: string[];
  /** Attribute relationships as [typeId, targetConceptId, roleGroup]. */
  attributes: Array<[string, string, number]>;
}

const concepts: Concept[] = [];
const byName = new Map<string, string>();

function define(
  name: string,
  fsn: string,
  preferredTerm: string,
  parents: string[],
  options: {
    active?: boolean;
    defined?: boolean;
    acceptableSynonyms?: string[];
    divergentTerm?: string;
    attributes?: Array<[string, string, number]>;
    /** An identifier to use instead of allocating a synthetic one, for a real metadata concept. */
    fixedId?: string;
  } = {},
): string {
  const id = options.fixedId ?? nextConceptId();
  byName.set(name, id);
  concepts.push({
    id,
    active: options.active ?? true,
    defined: options.defined ?? false,
    fsn,
    preferredTerm,
    acceptableSynonyms: options.acceptableSynonyms ?? [],
    divergentTerm: options.divergentTerm,
    parents,
    attributes: options.attributes ?? [],
  });
  return id;
}

// Clinical finding subtree (>= 5 hierarchy levels below the root).
const rootFinding = define(
  "ROOT_FINDING",
  "Mini clinical finding (finding)",
  "Mini clinical finding",
  [ROOT],
  { defined: false },
);
const disorder = define(
  "DISORDER",
  "Mini disorder (disorder)",
  "Mini disorder",
  [rootFinding],
);
const diabetes = define(
  "DIABETES",
  "Diabetes mellitus (disorder)",
  "Diabetes mellitus",
  [disorder],
  { defined: true },
);
const type1 = define(
  "TYPE1_DIABETES",
  "Type 1 diabetes mellitus (disorder)",
  "Type 1 diabetes mellitus",
  [diabetes],
  { defined: true },
);
const type2 = define(
  "TYPE2_DIABETES",
  "Type 2 diabetes mellitus (disorder)",
  "Type 2 diabetes mellitus",
  [diabetes],
  {
    defined: true,
    acceptableSynonyms: ["T2DM"],
  },
);
define(
  "TYPE2_WITH_COMPLICATION",
  "Type 2 diabetes mellitus with complication (disorder)",
  "Type 2 diabetes mellitus with complication",
  [type2],
);
const gestational = define(
  "GESTATIONAL_DIABETES",
  "Gestational diabetes mellitus (disorder)",
  "Gestational diabetes mellitus",
  [diabetes],
);
define(
  "HYPERTENSION",
  "Hypertensive disorder (disorder)",
  "Hypertensive disorder",
  [disorder],
);

// Body structure subtree (targets of the finding-site attribute).
const bodyStructure = define(
  "BODY_STRUCTURE",
  "Mini body structure (body structure)",
  "Mini body structure",
  [ROOT],
);
// The first of the three concepts the language reference sets disagree about.
const endocrineStructure = define(
  "ENDOCRINE_STRUCTURE",
  "Endocrine system structure (body structure)",
  "Endocrine system structure",
  [bodyStructure],
  { divergentTerm: "Structure of endocrine system" },
);
// The second of the three divergent concepts.
const pancreas = define(
  "PANCREAS_STRUCTURE",
  "Pancreatic structure (body structure)",
  "Pancreatic structure",
  [endocrineStructure],
  { divergentTerm: "Structure of pancreas" },
);

// Morphology subtree (targets of the associated-morphology attribute).
const morphology = define(
  "MORPHOLOGY_TOP",
  "Mini morphologic abnormality (morphologic abnormality)",
  "Mini morphologic abnormality",
  [ROOT],
);
// The third of the three divergent concepts.
const degeneration = define(
  "DEGENERATION_MORPH",
  "Degeneration (morphologic abnormality)",
  "Degeneration",
  [morphology],
  { divergentTerm: "Degenerative change" },
);

// An inactive concept with a historical SAME AS association to its active replacement.
const inactiveDiabetes = define(
  "DIABETES_INACTIVE",
  "Diabetes (disorder)",
  "Diabetes",
  [diabetes],
  { active: false },
);

// Attribute relationships, grouped in role group 1 (present but not queried in v1).
concepts
  .find((c) => c.id === diabetes)!
  .attributes.push(
    [FINDING_SITE, pancreas, 1],
    [ASSOCIATED_MORPHOLOGY, degeneration, 1],
  );
concepts
  .find((c) => c.id === type1)!
  .attributes.push([FINDING_SITE, pancreas, 1]);
concepts
  .find((c) => c.id === type2)!
  .attributes.push([FINDING_SITE, pancreas, 1]);

// Filler leaf disorders to give the closure a realistic size (targets ~200 concepts total).
const fillerDiabetes: string[] = [];
for (let i = 1; i <= 90; i++) {
  const label = `Mini diabetes subtype ${i}`;
  fillerDiabetes.push(
    define(`DIABETES_FILLER_${i}`, `${label} (disorder)`, label, [diabetes]),
  );
}
const fillerDisorder: string[] = [];
for (let i = 1; i <= 95; i++) {
  const label = `Mini other disorder ${i}`;
  fillerDisorder.push(
    define(`DISORDER_FILLER_${i}`, `${label} (disorder)`, label, [disorder]),
  );
}

// A concept representing the simple reference set (a subtype of the root).
const simpleRefset = define(
  "SIMPLE_REFSET",
  "Mini simple reference set (foundation metadata concept)",
  "Mini simple reference set",
  [ROOT],
);

// --- Reference set membership. ---

interface SimpleMember {
  refset: string;
  referenced: string;
}
interface LanguageMember {
  refset: string;
  description: string;
  acceptability: string;
}
interface AssociationMember {
  refset: string;
  referenced: string;
  target: string;
}

const simpleMembers: SimpleMember[] = [
  { refset: simpleRefset, referenced: type1 },
  { refset: simpleRefset, referenced: type2 },
  { refset: simpleRefset, referenced: gestational },
];

const associationMembers: AssociationMember[] = [
  { refset: SAME_AS_REFSET, referenced: inactiveDiabetes, target: type2 },
];

// --- RF2 line assembly. ---

const CRLF = "\r\n";

interface DescriptionRow {
  id: string;
  conceptId: string;
  typeId: string;
  term: string;
}

/**
 * Builds the description rows for a concept and the language refset rows that rank them, one set of
 * rankings per language reference set the release holds.
 *
 * Every reference set prefers the fully specified name and marks each additional synonym acceptable.
 * They differ only over a concept that declares a divergent term: a divergent reference set prefers
 * that term and merely accepts the default preferred term, while the others do the reverse.
 */
function descriptionsFor(
  concept: Concept,
  refsets: LanguageRefset[],
): {
  descriptions: DescriptionRow[];
  language: LanguageMember[];
} {
  const descriptions: DescriptionRow[] = [];
  const language: LanguageMember[] = [];

  /** Ranks one description within every language reference set of the release. */
  function rank(
    description: string,
    acceptabilityFor: (refset: LanguageRefset) => string,
  ): void {
    for (const refset of refsets) {
      language.push({
        refset: refset.id,
        description,
        acceptability: acceptabilityFor(refset),
      });
    }
  }

  const fsnId = nextDescriptionId();
  descriptions.push({
    id: fsnId,
    conceptId: concept.id,
    typeId: FSN,
    term: concept.fsn,
  });
  rank(fsnId, () => PREFERRED);

  const synId = nextDescriptionId();
  descriptions.push({
    id: synId,
    conceptId: concept.id,
    typeId: SYNONYM,
    term: concept.preferredTerm,
  });
  rank(synId, (refset) =>
    refset.divergent && concept.divergentTerm ? ACCEPTABLE : PREFERRED,
  );

  for (const extra of concept.acceptableSynonyms) {
    const extraId = nextDescriptionId();
    descriptions.push({
      id: extraId,
      conceptId: concept.id,
      typeId: SYNONYM,
      term: extra,
    });
    rank(extraId, () => ACCEPTABLE);
  }

  if (concept.divergentTerm !== undefined) {
    const divergentId = nextDescriptionId();
    descriptions.push({
      id: divergentId,
      conceptId: concept.id,
      typeId: SYNONYM,
      term: concept.divergentTerm,
    });
    rank(divergentId, (refset) => (refset.divergent ? PREFERRED : ACCEPTABLE));
  }
  return { descriptions, language };
}

function conceptFile(effectiveTime: string, module: string): string {
  const header = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "definitionStatusId",
  ].join("\t");
  const lines = [header];
  for (const c of concepts) {
    lines.push(
      [
        c.id,
        effectiveTime,
        c.active ? "1" : "0",
        module,
        c.defined ? DEFINED : PRIMITIVE,
      ].join("\t"),
    );
  }
  return lines.join(CRLF) + CRLF;
}

function descriptionAndLanguage(
  effectiveTime: string,
  module: string,
  refsets: LanguageRefset[],
): {
  description: string;
  language: string;
} {
  const descHeader = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "conceptId",
    "languageCode",
    "typeId",
    "term",
    "caseSignificanceId",
  ].join("\t");
  const langHeader = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "refsetId",
    "referencedComponentId",
    "acceptabilityId",
  ].join("\t");
  const descLines = [descHeader];
  const langLines = [langHeader];
  const caseSignificance = "900000000000448009"; // Entire term case insensitive.
  for (const c of concepts) {
    const { descriptions, language } = descriptionsFor(c, refsets);
    for (const d of descriptions) {
      descLines.push(
        [
          d.id,
          effectiveTime,
          "1",
          module,
          d.conceptId,
          "en",
          d.typeId,
          d.term,
          caseSignificance,
        ].join("\t"),
      );
    }
    for (const l of language) {
      langLines.push(
        [
          nextRefsetUuid(),
          effectiveTime,
          "1",
          module,
          l.refset,
          l.description,
          l.acceptability,
        ].join("\t"),
      );
    }
  }
  return {
    description: descLines.join(CRLF) + CRLF,
    language: langLines.join(CRLF) + CRLF,
  };
}

function relationshipFile(effectiveTime: string, module: string): string {
  const header = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "sourceId",
    "destinationId",
    "relationshipGroup",
    "typeId",
    "characteristicTypeId",
    "modifierId",
  ].join("\t");
  const lines = [header];
  const inferred = "900000000000011006"; // Inferred relationship.
  const some = "900000000000451002"; // Some (modifier).
  for (const c of concepts) {
    for (const parent of c.parents) {
      lines.push(
        [
          nextRelationshipId(),
          effectiveTime,
          c.active ? "1" : "0",
          module,
          c.id,
          parent,
          "0",
          IS_A,
          inferred,
          some,
        ].join("\t"),
      );
    }
    for (const [type, target, group] of c.attributes) {
      lines.push(
        [
          nextRelationshipId(),
          effectiveTime,
          "1",
          module,
          c.id,
          target,
          String(group),
          type,
          inferred,
          some,
        ].join("\t"),
      );
    }
  }
  return lines.join(CRLF) + CRLF;
}

function simpleRefsetFile(effectiveTime: string, module: string): string {
  const header = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "refsetId",
    "referencedComponentId",
  ].join("\t");
  const lines = [header];
  for (const m of simpleMembers) {
    lines.push(
      [
        nextRefsetUuid(),
        effectiveTime,
        "1",
        module,
        m.refset,
        m.referenced,
      ].join("\t"),
    );
  }
  return lines.join(CRLF) + CRLF;
}

function associationRefsetFile(effectiveTime: string, module: string): string {
  const header = [
    "id",
    "effectiveTime",
    "active",
    "moduleId",
    "refsetId",
    "referencedComponentId",
    "targetComponentId",
  ].join("\t");
  const lines = [header];
  for (const m of associationMembers) {
    lines.push(
      [
        nextRefsetUuid(),
        effectiveTime,
        "1",
        module,
        m.refset,
        m.referenced,
        m.target,
      ].join("\t"),
    );
  }
  return lines.join(CRLF) + CRLF;
}

// --- Emit a release. ---

const baseDir = dirname(new URL(import.meta.url).pathname);

function write(relativePath: string, content: string): void {
  const path = join(baseDir, relativePath);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, "utf8");
}

function emitRelease(
  release: string,
  effectiveTime: string,
  options: {
    module?: string;
    namespaceToken?: string;
    refsets?: LanguageRefset[];
  } = {},
): void {
  // The refset UUID sequence and description IDs are reset per release so that every
  // release carries a self-consistent, identical set of component identifiers.
  descriptionItem = 5000;
  relationshipItem = 80000;
  refsetSequence = 0;

  const module = options.module ?? CORE_MODULE;
  const token = options.namespaceToken ?? "INT";
  const refsets = options.refsets ?? INTERNATIONAL_REFSETS;
  const terminology = `${release}/Snapshot/Terminology`;
  const refset = `${release}/Snapshot/Refset`;
  write(
    `${terminology}/sct2_Concept_Snapshot_${token}_${effectiveTime}.txt`,
    conceptFile(effectiveTime, module),
  );
  const { description, language } = descriptionAndLanguage(
    effectiveTime,
    module,
    refsets,
  );
  write(
    `${terminology}/sct2_Description_Snapshot-en_${token}_${effectiveTime}.txt`,
    description,
  );
  write(
    `${terminology}/sct2_Relationship_Snapshot_${token}_${effectiveTime}.txt`,
    relationshipFile(effectiveTime, module),
  );
  write(
    `${refset}/Language/der2_cRefset_LanguageSnapshot-en_${token}_${effectiveTime}.txt`,
    language,
  );
  write(
    `${refset}/Content/der2_Refset_SimpleSnapshot_${token}_${effectiveTime}.txt`,
    simpleRefsetFile(effectiveTime, module),
  );
  write(
    `${refset}/Content/der2_cRefset_AssociationSnapshot_${token}_${effectiveTime}.txt`,
    associationRefsetFile(effectiveTime, module),
  );
}

// Clean any previous output.
for (const release of [
  "international-20230601",
  "international-20240601",
  "national-20240601",
]) {
  try {
    rmSync(join(baseDir, release), { recursive: true, force: true });
  } catch {
    // Nothing to remove.
  }
}

// The v1 release.
emitRelease("international-20230601", EFFECTIVE_TIME_V1);

// The v2 release adds one concept (a second gestational subtype) at a later effective time.
define(
  "GESTATIONAL_SUBTYPE",
  "Gestational diabetes mellitus in pregnancy (disorder)",
  "Gestational diabetes mellitus in pregnancy",
  [gestational],
);
emitRelease("international-20240601", EFFECTIVE_TIME_V2);

// --- The national release. ---
//
// A release in a module other than the International core, holding three language reference sets, so
// that no rule can choose a default dialect for it and the import must ask the operator instead. The
// module and the extension reference set carry synthetic SCTIDs, since no real national edition is
// being imitated; the two English reference sets are the International ones, which a national
// edition does inherit.
//
// The concepts naming the three reference sets are defined here, after both International releases
// have been emitted, so that only this release carries them. Their fully specified names are what
// the ambiguity failure quotes back to the operator.
const NATIONAL_MODULE = withCheckDigit("199900");
const NATIONAL_ENGLISH_REFSET = withCheckDigit("199901");

define(
  "GB_ENGLISH_REFSET_CONCEPT",
  "Great Britain English language reference set (foundation metadata concept)",
  "GB English",
  [ROOT],
  { fixedId: GB_ENGLISH_REFSET },
);
define(
  "US_ENGLISH_REFSET_CONCEPT",
  "United States of America English language reference set (foundation metadata concept)",
  "US English",
  [ROOT],
  { fixedId: US_ENGLISH_REFSET },
);
define(
  "NATIONAL_ENGLISH_REFSET_CONCEPT",
  "Mini national English language reference set (foundation metadata concept)",
  "Mini national English",
  [ROOT],
  { fixedId: NATIONAL_ENGLISH_REFSET },
);

emitRelease("national-20240601", EFFECTIVE_TIME_V2, {
  module: NATIONAL_MODULE,
  namespaceToken: "MINI",
  refsets: [
    { id: US_ENGLISH_REFSET, divergent: false },
    { id: GB_ENGLISH_REFSET, divergent: true },
    { id: NATIONAL_ENGLISH_REFSET, divergent: false },
  ],
});

const totalConcepts = concepts.length;
const activeConcepts = concepts.filter((c) => c.active).length;
console.log(
  `Generated rf2-mini: ${totalConcepts} concepts (${activeConcepts} active) in the national release.`,
);
console.log(`National module: ${NATIONAL_MODULE}`);
console.log(`National language reference set: ${NATIONAL_ENGLISH_REFSET}`);
console.log("Well-known codes:");
for (const [name, id] of byName) {
  console.log(`  ${name} = ${id}`);
}
