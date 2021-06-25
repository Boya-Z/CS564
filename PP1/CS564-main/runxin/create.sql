DROP TABLE if exists Category;
DROP TABLE if exists Bid;
DROP TABLE if exists User;
DROP TABLE if exists Item;


CREATE TABLE Category
(
 item_id        INT          NOT NULL,
 category_name VARCHAR(255)  NOT NULL,
 PRIMARY KEY (item_id, category_name)
 FOREIGN KEY (item_id) REFERENCES Item
);


CREATE TABLE Bid
(
 item_id        INT          NOT NULL ,
 bidder_id      VARCHAR(255) NOT NULL,
 time           datetime     NOT NULL,
 amount         DOUBLE       NOT NULL,
 PRIMARY KEY(item_id, bidder_id , amount),
 FOREIGN KEY (item_id) REFERENCES Item,
 FOREIGN KEY (bidder_id) REFERENCES User(user_id)
);


CREATE TABLE User
(
 user_id     VARCHAR(255)    PRIMARY KEY NOT NULL ,
 rating      INT             NOT NULL,
 location    VARCHAR(255),
 country     VARCHAR(255),
 UNIQUE (user_id,location,country)

);
 

CREATE TABLE Item
(
 item_id        INT          PRIMARY KEY NOT NULL ,
 name           VARCHAR(255) NOT NULL,
 currently      DOUBLE,
 buy_price      DOUBLE,
 first_bid      DOUBLE,
 number_of_bids INT          NOT NULL,
 started        datetime     NOT NULL,
 ends           datetime     NOT NULL,
 description    VARCHAR(500),
 seller_id      VARCHAR(255) NOT NULL,
 FOREIGN KEY (seller_id) REFERENCES User(user_id)
);
