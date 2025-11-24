-- models/raw_stocks_data.sql


SELECT
    SYMBOL,
    DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME
FROM USER_DB_MARMOT.RAW.STOCKS_INFO_LAB_1