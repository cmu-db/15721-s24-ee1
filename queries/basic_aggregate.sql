SELECT
    SUM(orders.o_totalprice), orders.o_shippriority
FROM
    orders
WHERE
    orders.o_shippriority > 2
GROUP BY orders.o_shippriority
;
