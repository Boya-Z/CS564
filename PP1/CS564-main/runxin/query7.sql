SELECT COUNT (distinct category_name) FROM Category
WHERE item_id in (SELECT distinct item_id FROM Bid WHERE amount >100);
