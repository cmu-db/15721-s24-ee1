SELECT
    o_totalprice, c_custkey, o_orderdate
FROM 
    orders, customer
WHERE
    orders.o_custkey = customer.c_custkey AND
    orders.o_totalprice < 900.00
;
