--  having following tables
--  products (id, product_name, price, category,supplier_id)
--  orders (id, customer_id, quantity, date, hour)
--  order_items (order_id, product_id, quantity)
--  customers (id, name, age, governorate,gender)
--  suppliers (id, name)
--  ratings (customer_id, product_id, rating)-- 

-- query 1
select suppliers.name
from suppliers
join products on suppliers.id = products.supplier_id
join ratings on products.id = ratings.product_id
group by suppliers.name
having avg(Cast(rating as Float)) >= 3.1

-- query 2
-- NOT Optimized
select customers.name
from customers
join orders on customers.id = orders.customer_id
join order_items on orders.id = order_items.order_id
join products on order_items.product_id = products.id
group by customers.id
having sum(products.price * order_items.quantity) > 100

-- Optimized
select customers.name
from customers
join orders on customers.id = orders.customer_id
group by customers.id
having sum(orders.total_price) > 100


-- query 3
-- part one fetch the need supplier name via:
select count(products.id), suppliers.name
from suppliers
join products on suppliers.id = products.supplier_id
group by (suppliers.id)

-- part two use it  in the following query
select product_name, sum(quantity) as total_quantity
from products
join order_items on products.id = order_items.product_id
join suppliers on products.supplier_id = suppliers.id
where suppliers.name = ‘Maurice Wallace1715’
group by product_name
order by total_quantity desc
limit 1;

-- its Optimization was done by adding an index on the suppliers.name using
create index suppliers_name_index on suppliers (name);


-- query 4
select count(customer_order.id) as customers_count
from (
select count(customers.id) as total_record, customers.id, customers.name
from customers
join orders on orders.customer_id = customers.id
join order_items on order_items.order_id = orders.id
where age>25 and age<35
group by customers.id
having count(DISTINCT orders.id) >= 2
and count(order_items.product_id) >= 3) customer_order;

-- its Optimization was don by adding an index on the customers.age using
create index customers_age_index on customers (age);



