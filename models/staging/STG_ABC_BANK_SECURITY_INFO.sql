{{config(materialized='ephemeral')}}

WITH
src_data as (
    SELECT 
    ,SECURITY_CODE as SECURITY_CODE 
    ,SECURITY_NAME as SECURITY_NAME
    ,SECTOR as SECTOR_NAME
    ,INDUSTRY as INDUSTRY_NAME
    ,COUNTRY as COUNTRY_CODE
    ,EXCHANGE as EXCHANGE_CODE
    ,LOAD_TD as LOAD_TS 
    ,'SEED_ABC_Bank_SECURITY_INFO' as RECORD_SOURCE
 from {{source("seeed","ABC_Bank_SECURITY_INFO")}}

),
hashed as (
    SELECT 
        concat_ws('|',SECURITY_CODE) as SECURITY_HKEY
        ,concat_ws('|',SECURITY_CODE,SECURITY_NAME,SECTOR_NAME,INDUSTRY_NAME,COUNTRY_CODE,EXCHANGE_CODE) as SECURITY_HDIFF
        , exclude LOAD_TS
        ,LOAD_TS as LOAD_TS_UTC
    from src_data

)
select from hashed