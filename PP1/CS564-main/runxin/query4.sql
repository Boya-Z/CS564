SELECT item_id FROM Item WHERE Item.currently == (SELECT MAX(currently) FROM Item);