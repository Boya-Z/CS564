select count(user_id)
from User
where user_id in (
    SELECT seller_id
    FROM Item
) and rating > 1000