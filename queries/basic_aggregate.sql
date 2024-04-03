SELECT
    SUM(orders.o_totalprice)
FROM
    orders
GROUP BY orders.o_shippriority
;
