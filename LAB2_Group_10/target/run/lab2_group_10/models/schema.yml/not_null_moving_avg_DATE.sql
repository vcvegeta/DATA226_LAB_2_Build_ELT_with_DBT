select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select DATE
from USER_DB_MARMOT.analytics.moving_avg
where DATE is null



      
    ) dbt_internal_test