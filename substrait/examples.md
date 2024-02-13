

# basic_query

```
CREATE TABLE Authors (
  author_id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  nationality VARCHAR(50)
);

CREATE TABLE Books (
  book_id INT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  author_id INT NOT NULL,
  publication_year INT DEFAULT 0,
  FOREIGN KEY (author_id) REFERENCES Authors(author_id)
);

INSERT INTO Authors (author_id, name, nationality) VALUES
(1, 'Jane Austen', 'England'),
(2, 'William Shakespeare', 'England'),
(3, 'Victor Hugo', 'France'),
(4, 'Fyodor Dostoevsky', 'Russia');

INSERT INTO Books (book_id, title, author_id, publication_year) VALUES
(1, 'Pride and Prejudice', 1, 1813),
(2, 'Hamlet', 2, 1603),
(3, 'Les Misérables', 3, 1862),
(4, 'Crime and Punishment', 4, 1866);

SELECT
  a.name AS author_name,
  b.title AS book_title
FROM Authors a
JOIN Books b ON a.author_id = b.author_id
WHERE a.nationality = 'England' AND b.publication_year < 1850;
```


# TPC-H Query 1

lineitem
```
CREATE TABLE lineitem (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber INTEGER NOT NULL,
  l_quantity INTEGER NOT NULL,
  l_shipdate DATE NOT NULL,
  l_discount DOUBLE PRECISION NOT NULL,
  l_tax DOUBLE PRECISION NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(2) NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
```

# orders
```
CREATE TABLE orders (
o_orderkey BIGINT NOT NULL PRIMARY KEY,
o_custkey BIGINT NOT NULL,
o_orderstatus CHAR(1) NOT NULL,
o_totalprice DECIMAL NOT NULL,
o_orderdate DATE NOT NULL,
o_orderpriority CHAR(15) NOT NULL,
o_clerk CHAR(15) NOT NULL,
o_shippriority INTEGER NOT NULL,
o_comment VARCHAR(79) NOT NULL,
);
```