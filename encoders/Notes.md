Dev notes 
---------


## References


Debugging spark code generation

- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-CodeGenerator.html
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-debugging-query-execution.html



## Generate code 

    // final CodegenContext codeGenContext = new CodegenContext();
    //
    // ExprCode code = resolvedPatientEncoder
    //     .objSerializer().genCode(codeGenContext);
    
    
    
### Manual deserialization

    // The encoder needs to be resolvedAndBound
    // before the deserializer() can be used.
    
    final ExpressionEncoder<Observation> patientEncoder = fhirEncoders
        .of(Observation.class);
    ExpressionEncoder<Observation> resolvedPatientEncoder = EncoderUtils
        .defaultResolveAndBind(patientEncoder);
        
    Observation deserializedPatient = resolvedPatientEncoder.createDeserializer().apply(serializedRow);
    assertTrue(patient.equalsDeep(deserializedPatient));
