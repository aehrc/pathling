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

package au.csiro.pathling.operations.sqlquery;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer;
import org.apache.spark.sql.catalyst.parser.UpperCaseCharStream;

/**
 * The text of a SQL query, together with a view of the tokens Spark's own lexer produces for it.
 * {@link SqlLabelRewriter} uses the tokens to place the table alias it injects after a substituted
 * relation reference, which is the one thing the parsed plan cannot tell it.
 *
 * <p>The query is lexed at most once per instance, and only if the tokens are actually asked for.
 *
 * @author John Grimes
 */
class SqlSource {

  /** Returned by {@link #aliasInsertionPoint} when the grammar admits no table alias. */
  static final int NO_ALIAS = -1;

  @Nonnull private final String text;

  /** The default-channel tokens of the query, lexed on first use. */
  @Nullable private List<Token> tokens;

  SqlSource(@Nonnull final String text) {
    this.text = text;
  }

  /**
   * Returns the run of query text between two offsets.
   *
   * @param beginIndex the offset of the first character, inclusive
   * @param endIndex the offset just past the last character
   * @return the text between the offsets
   */
  @Nonnull
  String substring(final int beginIndex, final int endIndex) {
    return text.substring(beginIndex, endIndex);
  }

  /**
   * Returns the offset at which a table alias may be inserted for the relation reference occupying
   * the given span, or {@link #NO_ALIAS} when the grammar admits no alias for that reference.
   *
   * <p>The rule is {@code identifierReference temporalClause? optionsClause? sampleClause?
   * tableAlias}, so the alias goes last, after any options and sample clauses. Neither clause can
   * be measured from a plan node: the options end up in a field of the relation and produce no node
   * at all, and {@code TABLESAMPLE (n ROWS)} produces a limit rather than a {@code Sample}. Both
   * are therefore measured here as a keyword immediately followed by a balanced parenthesis group.
   * Demanding that parenthesis is what stops the {@code WITH} of a common table expression being
   * taken for an options clause.
   *
   * <p>Two positions admit no alias at all. The target of a {@code TABLE} query primary, whose rule
   * is {@code TABLE identifierReference}, has no alias slot; the form is invisible in the parsed
   * plan, because {@code TABLE age} and {@code FROM age} yield the same relation node under the
   * same parents, so it is recognised from the preceding token instead. A temporal clause ({@code
   * VERSION AS OF} and its variants) is refused rather than measured: time travel is rejected
   * before the rewriter runs, and declining an alias costs only the resolution of a label-qualified
   * column, whereas guessing an insertion point could produce text that does not parse.
   *
   * <p>Whitespace and both comment forms are lexed onto the hidden channel, so working over the
   * default channel steps across them in both directions: a comment between {@code TABLE} and the
   * identifier does not hide the keyword, and the word {@code TABLE} within a comment is never
   * mistaken for it.
   *
   * @param start the offset of the first character of the relation identifier
   * @param stop the offset of the last character of the relation identifier
   * @return the offset just past the end of the relation primary, or {@link #NO_ALIAS}
   */
  int aliasInsertionPoint(final int start, final int stop) {
    final List<Token> tokenList = tokens();
    Token preceding = null;
    int next = tokenList.size();
    for (int i = 0; i < tokenList.size(); i++) {
      final Token token = tokenList.get(i);
      if (token.getStopIndex() < start) {
        preceding = token;
      } else if (token.getStartIndex() > stop) {
        next = i;
        break;
      }
    }
    if (preceding != null && preceding.getType() == SqlBaseLexer.TABLE) {
      return NO_ALIAS;
    }

    int insertion = stop + 1;
    int index = next;
    while (index < tokenList.size()) {
      final int type = tokenList.get(index).getType();
      if (startsTemporalClause(type)) {
        return NO_ALIAS;
      }
      if ((type != SqlBaseLexer.WITH && type != SqlBaseLexer.TABLESAMPLE)
          || index + 1 >= tokenList.size()
          || tokenList.get(index + 1).getType() != SqlBaseLexer.LEFT_PAREN) {
        break;
      }
      final int close = closingParenthesis(tokenList, index + 1);
      if (close == NO_ALIAS) {
        // The text has already been parsed, so the group is balanced; an unbalanced one can only
        // mean the token view disagrees with the parse, and stopping short is the safe response.
        break;
      }
      insertion = tokenList.get(close).getStopIndex() + 1;
      index = close + 1;
    }
    return insertion;
  }

  /** Reports whether the token type opens the temporal clause of a relation primary. */
  private static boolean startsTemporalClause(final int type) {
    return type == SqlBaseLexer.FOR
        || type == SqlBaseLexer.VERSION
        || type == SqlBaseLexer.SYSTEM_VERSION
        || type == SqlBaseLexer.TIMESTAMP
        || type == SqlBaseLexer.SYSTEM_TIME;
  }

  /**
   * Returns the index of the token closing the parenthesis group opened at the given index, or
   * {@link #NO_ALIAS} when the group is not closed.
   */
  private static int closingParenthesis(@Nonnull final List<Token> tokenList, final int openIndex) {
    int depth = 0;
    for (int i = openIndex; i < tokenList.size(); i++) {
      final int type = tokenList.get(i).getType();
      if (type == SqlBaseLexer.LEFT_PAREN) {
        depth++;
      } else if (type == SqlBaseLexer.RIGHT_PAREN) {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return NO_ALIAS;
  }

  /**
   * Lexes the query on first use. The lexer is Spark's own, over the same upper-casing character
   * stream Spark itself wraps the input in, because the grammar's keyword rules match uppercase
   * text only. Error listeners are removed because the caller has already parsed the same text
   * successfully, so there is nothing left to report.
   */
  @Nonnull
  private List<Token> tokens() {
    if (tokens == null) {
      final SqlBaseLexer lexer =
          new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(text)));
      lexer.removeErrorListeners();
      final CommonTokenStream stream = new CommonTokenStream(lexer);
      stream.fill();
      final List<Token> defaultChannel = new ArrayList<>();
      for (final Token token : stream.getTokens()) {
        if (token.getChannel() == Token.DEFAULT_CHANNEL) {
          defaultChannel.add(token);
        }
      }
      tokens = defaultChannel;
    }
    return tokens;
  }
}
