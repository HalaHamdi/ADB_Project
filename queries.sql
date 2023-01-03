--  write me a sql query to get  the 10 most selling products for supplier with id 1
--  i have the following tables
--  products (id, product_name, price, category,supplier_id)
--  orders (id, customer_id, quantity, date, hour)
--  order_items (order_id, product_id, quantity)
--  customers (id, name, age, governorate,gender)
--  suppliers (id, name)
--  ratings (customer_id, product_id, rating)-- 
--  i want to get the 10 most selling products for supplier with id 1

select product_name, sum(quantity) as total_quantity
from products, suppliers, order_items
join order_items on products.id = order_items.product_id
join suppliers on products.supplier_id = suppliers.id
where supplier_id = 1
group by product_name
order by total_quantity desc
limit 10