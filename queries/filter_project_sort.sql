SELECT orders.o_totalprice, orders.o_orderdate
FROM orders
WHERE
    orders.o_totalprice < 900.00
ORDER BY orders.o_totalprice;