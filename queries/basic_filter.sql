SELECT
    orders.o_totalprice
FROM
    orders
WHERE
    orders.o_totalprice < 900.00
;
