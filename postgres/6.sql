-------------- Values for the following substitution parameters must be generated and used to build the executable query text:
--1. DATE is the first of January of a randomly selected year within [1993 .. 1997];
--2. DISCOUNT is randomly selected within [0.02 .. 0.09];
--3. QUANTITY is randomly selected within [24 .. 25].


select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= date('1994-01-01')
        and l_shipdate < date('1994-01-01') + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24;
