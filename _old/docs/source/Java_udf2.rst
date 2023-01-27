Java UDF to output GC content to table
======================================

Input
------
Column Syntax - ``.withColumn (colName: String, col: Column)`` used to change the value, convert the datatype of an existing column, create a new column,

| ``colName:String`` – specify a new column you wanted to create. use an existing column to update the value.

| ``col:Column`` – column expression.

``DataFrame.createOrReplaceTempView(name)`` - Creates or replaces a local temporary view with this DataFrame




.. code-block::

    import pyspark.sql.functions as F

    read='dbfs:/52707.2.423369.GTCTAGGT-GTCTAGGT.fastq.gz'
    input_data = (spark.read.text(read, lineSep='@')
        .filter(F.length('value')>1)
    )
    input_seq = (input_data
        .withColumn('name', F.split('value', '\n').getItem(0))
        .withColumn('seq', F.split('value', '\n').getItem(1))
        .withColumn('qual', F.split('value', '\n').getItem(3))
        .withColumn("id", F.monotonically_increasing_id())
        .withColumn("sid", F.lit('s01'))
        .select('id', 'name', 'sid', 'seq', 'qual')
    )
    # access the dataframe via SQL interface, create a table named "reads" for us to do SQL
    input_seq.createOrReplaceTempView('reads')
    # this is the SQL select syntax
    sql_cmd = """SELECT id, name, qual, seq, getGC(seq) as GC3 from reads"""
    input_seq = spark.sql(sql_cmd)
    input_seq.show(truncate=False, vertical=True)
    input_seq.select(F.avg('GC3')).collect()

Output:

.. Code-block::
    
     -RECORD 0-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 0                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:6460:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFFFFFFFF:FFF,FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF::FF,FFFFFFFFF:,FFFFFFFFFFFF 
    seq  | CTATGACGAACCATGAAAAGCTCACTTCCACCTCTTTGCATTTTTAAGCGACTTCGATTTTTGCTTGCTTTGTTGGAAGGCCTGAACCTTCTGCTTCTTTCTGACAGCTTTCTCTGCCTTCGACATTTTGGATTTGGCCTTTACTTTGGGG 
    GC3  | 43.70860927152318                                                                                                                                       
    -RECORD 1-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 1                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:6460:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFF:FFFFFFFFF:FFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFF,FF:FFFFFFFFFFFFFFFF,FFFFFFFFFFFFFF,FFFFFF:FFFFFFF,FFF:FFF:::FFFFFFFFFFFF:FFFFF 
    seq  | GACAAACCATCTTGATATCCCATCAAAAGAAATGCTTGAGGAGGCAATATCAGAATATACAGGTACTGTGATAACAGTTTCTCATGATCGGTACTTCATAAAACAAAGTGTTAACCGAGTCATTGAAGTGAAAGATCAGACTATCCAGGAC 
    GC3  | 37.74834437086093                                                                                                                                       
    -RECORD 2-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 2                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:8034:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
    seq  | CCCCATTCCGGCGCTCGCCCACGTCGCCCCGCTCCGCCCCCTTGTGGTGTGCCCGCCGGAGTTCTTCGCCGGAGGCTCGCTGGCTCCGGCGAGGGGTCCCGGCGAGGTGGTGGTGGGTGGTGAGACGAAGCAGAGACGCGGGCCTCAGGGA 
    GC3  | 74.17218543046357                                                                                                                                       
    -RECORD 3-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 3                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:8034:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFF 
    seq  | GTAGTTGTTGTTCCCCTGTCCCTGAGGCCCGCGTCTCTGCTTCGTCTCACCACCCACCACCACCTCGCCGGGACCCCTCGCCGGAGCCAGCGAGCCTCCGGCGAAGAACTCCGGCGGGCACACCACAAGGGGGCGGAGCGGGGCGACGTGG 
    GC3  | 70.86092715231788                                                                                                                                       
    -RECORD 4-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 4                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:9968:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFF:FFFF:F:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FFFFFFFFFFFF 
    seq  | GGGTACTTAATTAAACAGCGGCAATGCAGCATCATCTCAGTGCGAGGACGGTGGTGGTGATGGTAGGTATACTAGAAGGTAGAGCTATCGAAATCCAGGAACATTGGCTCTGGTCGCTCCCGATCATCAGCAGGCGCATCCGATCGAGAGC 
    GC3  | 52.317880794701985                                                                                                                                      
    -RECORD 5-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 5                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:9968:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                           
    qual | FFFFFFFFFFFFFFFFFFFFFFF,FFFFFFFFFFFFF:FF,FFFFFFFFFFFFFFFFFFFFFFFFFF,FFFFFFFFFF:FF:FFFFF,FFFFFFFFFFFFFFFFFFFFFFFF,:FFFFFFFFFFFFFFFFFFFF,FFF:,FFFFFFFFF:: 
    seq  | CGAAGGAATCCGGAGAGGGTCAGTAAGATGGATCCAAGCTCTCGATCGGATGCGCCTGCTGATGATCGGGAGCGACCAGAGCCAATGTTCCTGGATTTCGATAGCTCTACCTTCTAGTATACCTACCATCACCACCACCGTCCTCGCACTG 
    GC3  | 54.3046357615894                                                                                                                                        
    -RECORD 6-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 6                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:11397:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
    seq  | GCCTGGACTGTTAGATAATTAAACATGAACAGATATTTGTATACATCAAATATTTGCAAATTCATGTGGTGATGCACATATTTTTTTGCATGCCAAATAATTTTTTATGTTAGTCAACGTACAATGCAAGTGTTTGCCATTCACGATCAAA 
    GC3  | 31.125827814569536                                                                                                                                      
    -RECORD 7-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 7                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:11397:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FF:FFFFFF:FFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
    seq  | GGCAAACACTTGCATTTGATCGTGAATGGCAAACACTTGCATTGTACGTTGACTAACATAAAAAATTATTTGGCATGCAAAAAAATATGTGCATCACCACATGAATTTGCAAATATTTGATGTATACAAATATCTGTTCATGTTTAATTAT 
    GC3  | 30.4635761589404                                                                                                                                        
    -RECORD 8-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 8                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:11686:1000 1:N:0:GTCTAGGT+GTCTAGGG                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
    seq  | GCGTCAACAATGAACGGATAACCAAGGAAAACCTTTCTGGAATGCGGATCGGGTAGTTGTAGACGAGCTGATTGAACTTCCCAGTCACACTCCGGAAATTGAAATCTGCCAAGCCTTTTCCAGCTGAGTTTTGCCAAATGGCTTCCAGAGC 
    GC3  | 47.682119205298015                                                                                                                                      
    -RECORD 9-------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 9                                                                                                                                                       
    name | A00178:342:HNNTMDSX3:2:1101:11686:1000 2:N:0:GTCTAGGT+GTCTAGGG                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFF 
    seq  | AGGCTATAAAAGAAGCTGGAATTGATGTGGAAGGCTTTCTCACAGTTGGACAAAATAAACAAATCCTAATCGATGCTGTTGTTCATGCTGTTAATGAAGACTATGCTGAAATGGCAAATGACTTCACTAGGCTGGGTTTTCTCGCTAGAGG 
    GC3  | 40.397350993377486                                                                                                                                      
    -RECORD 10------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 10                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:12138:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFF 
    seq  | GTCATCCATTAGGTCGGCTGGGAGGTTCTTCAAGAACCATGGATGGTTTCTTATCTCAGGGATGGTGATTCTAGTGGCTGGGTTGGCAACAAAAATCCTAGAAATAAGATCTTGGCACTCTGGAGATATATGGACATAATCTGGAATTGAG 
    GC3  | 43.70860927152318                                                                                                                                       
    -RECORD 11------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 11                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:12138:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFF:FFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFF:FF:FFFFF:FFFFFFFFF,FFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFF:FFFFFFFF 
    seq  | GTTGCTGACGTGTGGTCTTGCGGAGTAACCCTTTATGTGATGCTGGTTGGTGCATATCCATTTGAGGACCCAGATGAGCCCAAGAATTTCAGAAAGACAATTCAGAGAATATTGGGTGTGCAGTACTCAATTCCAGATTATGTCCATATAT 
    GC3  | 43.04635761589404                                                                                                                                       
    -RECORD 12------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 12                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:13367:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FFFFFFFFFFFFFFF: 
    seq  | CACGAGAGAGGGACCATGAAGAGGAGGGCTTGAATACATAGGCCTAATCACAAGCTTGAGTTGACTTTCAACCCTAACAGCTACATCAGCACTTCCACATACAATGCTCAAAGCACCAACACGCTCTCCATACAATCCCATGTTCTTAGCG 
    GC3  | 47.019867549668874                                                                                                                                      
    -RECORD 13------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 13                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:13367:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFF:FFFFFFFFFFFFFFFFFF:F:FFFFF:FFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFF,F:FFFF:FFF,FFFFFFFFFFFFFFFFF,FFFFF:F,,FFFFFFFFFFF,FF:FFFFFFFFFFFFFFFFFFFFF,FFFF 
    seq  | GTGATCCTGCAACCCGTGGTCTTGATTTCAACGGACTCATGGAAGACCTCAGTTCTGCTCCTTTAGGATCAATTGTACTGCTGCATGCTTGTGCCCATAACCCTACTGGGGTAGATCCTACCATTGATCAGTGGGAACAGATTAGGCAGCT 
    GC3  | 49.00662251655629                                                                                                                                       
    -RECORD 14------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 14                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:13711:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
    seq  | GTACACGCAGGGCCTTGAGCCCCTTAGCTCCTGCAGCTATCCAAAGCTGCAATTCATCCTTATAGGCTTGCAAAGCAACTTGGAGGCAGGGAGCTAGTCCATCAAAGACACATGCATTACGGTGTTTCCAAATGATCCAAGCACCTAGCAT 
    GC3  | 49.66887417218543                                                                                                                                       
    -RECORD 15------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 15                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:13711:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFF:FFFFFFFFFFFFFFF:FFFFFFFFF:FFFFFFF:FFF 
    seq  | TGGTGGTTGAGAGCTTGGAGGAAAGTCCCTAAACAGCACAAAAAAGGTTTCAATTCACTGGTCATGCTAGGTGCTTGGATCATTTGGAAACACCGTAATGCATGTGTCTTTGATGGACTAGCTCCCTGCCTCCAAGTTGCTTTGCAAGCCT 
    GC3  | 46.35761589403973                                                                                                                                       
    -RECORD 16------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 16                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:14181:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFF 
    seq  | GCCGATGAGGAGGTGCTCTCAGATGAGCACCTGAGCTGCAGCCAGCCTAGCAATCGGAACCCTGAAAGGGGAGCAAGAAACATAATCCAGCCCAGCCTTTGCGAAGAAAGCAACTGACTGAGGCTCCCCACCATGTTCTCCACAGATGCCC 
    GC3  | 56.29139072847682                                                                                                                                       
    -RECORD 17------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 17                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:14181:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FFFFF,FFFF:FFFF:FFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,FFFFFFF,FFFFFF 
    seq  | GTGGGGAAGTTTCTCCCCATTTATCTTTCTCAGGGTATCCTCCAACATGATCCCTTTGAGGTGCTTGATCAGAGAGGAGTGGGCGAGCTGGTTAAGTTTGCTACAGAGAGGGGACGCAAAACTAGGCCTAACCTGAAGGTGGGCATCTGTG 
    GC3  | 50.99337748344371                                                                                                                                       
    -RECORD 18------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 18                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:16351:1000 1:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FF,FFFFFFFFFFFF,FFFFFFFF,FFFFFFFFFFFFFF,FF,FFFFF:FFFFFFFFF:FFFFFFFFFFFFFFFFF,FFFFFFFFFFFFFFFFFFF 
    seq  | CCCGCAGACCCGAGCGAAAGCGGCGGTCCTTACAAGTCCGCTCCTCGGGGAGCTTGATTGATAATTCTGTATAAGGTGATCGCAGGTTGTGCAATCATTGCTCAAAAGGGTGTACACCGCCCTTAGACGTCTTGGTATACGGACAACTGAT 
    GC3  | 52.317880794701985                                                                                                                                      
    -RECORD 19------------------------------------------------------------------------------------------------------------------------------------------------------
    id   | 19                                                                                                                                                      
    name | A00178:342:HNNTMDSX3:2:1101:16351:1000 2:N:0:GTCTAGGT+GTCTAGGT                                                                                          
    qual | :FFFFFFFFFFFFF:FFFF:FFFFFFFFFFFFFFFFFFFFF,FFF:FFFFFFF::F,FFFFFFFFFFFFFFFFFFF:FF:FFFFFFFFFFFFFFFFFF:FFFFFFF:::FFFFFFFF::FFFFFFF,FF,FFFFFFFFFFFFFFFF:FFFF 
    seq  | GCCAGAATTATTAACTGCGCAGTTAGGGCAGCGTCTGAGGAAGTTTGCTGCGGTTTCGCCTTGACCGCGGGAAGGAGACATAACGATAGCGACTCTGTCTCAGGGGATCTGCATATGTTTGCAGCATACTTTAGGTGGGCCTTGGCTTCCT 
    GC3  | 52.317880794701985                                                                                                                                      
    only showing top 20 rows

    Out[16]: [Row(avg(GC3)=48.56027896521049)]

Explanation
------------

.. code-block::

    input_seq.createOrReplaceTempView('reads')
    sql_cmd = """SELECT id, name, qual, seq, getGC(seq) as GC3 from reads"""
    input_seq = spark.sql(sql_cmd)
    input_seq.show(truncate=False, vertical=True)
    input_seq.select(F.avg('GC3')).collect()



Accessing the dataframe via SQL interface, create a table named "reads" for us to do SQL

    * ``sql_cmd`` is used to load input data in a way that sql can read it 
    * This is the SQL select syntax and we're defining the sql ccommand
    * ``input_seq = spark.sql(sql_cmd)`` This is loading the sql command from spark
    * ``truncate`` - Through this parameter we can tell the Output sink to display the full column content by setting truncate option to false, by default this value is true
    * ``vertical`` - If set to True, print output rows vertically (one line per column value).
    * ``F.avg`` is used to find the average of 'GC3' quality scores and ``.collect`` Returns all the records as a list of Row.