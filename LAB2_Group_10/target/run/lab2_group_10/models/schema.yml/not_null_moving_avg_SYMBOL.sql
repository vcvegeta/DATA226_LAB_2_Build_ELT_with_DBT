select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select SYMBOL
from USER_DB_MARMOT.analytics.moving_avg
where SYMBOL is null



      
    ) dbt_internal_test