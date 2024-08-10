select
    *,
    date_part(year, current_date) - date_part(year, birth_date) employee_age,
    date_part(year, current_date) - date_part(year, hire_date) service_time,
    first_name || ' ' || last_name as full_name
from {{ source("sources", "employees") }}
