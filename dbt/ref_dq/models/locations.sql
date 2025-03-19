with LOCATIONS_REFS as (
    select *,  
    from
        locations_raw
)   

select NAME,CODE,HIERARCHY,IS_GROUP,CHILDREN, LEVEL_NAME from LOCATIONS_REFS