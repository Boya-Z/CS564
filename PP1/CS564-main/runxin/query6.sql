SELECT COUNT(user_id) FROM User 
WHERE user_id IN (SELECT seller_id FROM Item) AND user_id IN (SELECT bidder_id FROM Bid);