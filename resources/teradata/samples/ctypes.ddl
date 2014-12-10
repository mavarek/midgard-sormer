CREATE SET TABLE pp_scratch.lc_ctypes ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      c_bigint BIGINT,
      c_byte BYTE(1),
      c_byteint BYTEINT,
      c_char CHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_date DATE FORMAT 'YY/MM/DD',
      c_decimal DECIMAL(18,4),
      c_float FLOAT,
      c_integer INTEGER,
      c_smallint SMALLINT,
      c_time TIME(0),
      c_time6 TIME(6),
      c_timestamp TIMESTAMP(0),
      c_timestamp6 TIMESTAMP(6),
      c_varbyte VARBYTE(10),
      c_varchar VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC)
UNIQUE PRIMARY INDEX ( c_bigint, c_char );

