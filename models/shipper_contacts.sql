select sh.shipper_id, sh.company_name, sh.phone, se.email from {{source('sources','shippers')}} sh
left join {{ref('shipper_emails')}} se on (sh.shipper_id = se.shipper_id)