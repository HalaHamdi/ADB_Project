// change the following  sql query to mongodb
// select suppliers.name
// from suppliers
// join products on suppliers.id = products.supplier_id
// join ratings on products.id = ratings.product_id
// group by suppliers.name
// having avg(Cast(rating as Float)) >= 3.1 

// write the mongodb query here
db.suppliers.aggregate([
    {
        $lookup: {
            from: "products",
            localField: "id",
            foreignField: "supplier_id",
            as: "products"
        }
    },
    {
        $unwind: "$products"
    },
    {
        $lookup: {
            from: "ratings",
            localField: "products.id",
            foreignField: "product_id",
            as: "ratings"
        }
    },
    {
        $unwind: "$ratings"
    },
    {
        $group: {
            _id: "$name",
            avgRating: {
                $avg: {
                    $convert: {
                        input: "$ratings.rating",
                        to: "double"
                    }
                }
            }
        }
    },
    {
        $match: {
            avgRating: {

                $gte: 3.1

            }
        }
    }
])

// change the following  sql query to mongodb
// select customers.name
// from customers
// join orders on customers.id = orders.customer_id
// group by customers.id
// having sum(orders.total_price) > 100

// write the mongodb query here
db.customers.aggregate([
    {
        $lookup: {
            from: "orders",
            localField: "id",
            foreignField: "customer_id",
            as: "orders"
        }
    },
    {
        $unwind: "$orders"
    },
    {
        $group: {
            _id: "$id",
            total_price: {
                $sum: "$orders.total_price"
            }
        }
    },
    {
        $match: {
            total_price: {
                $gt: 100
            }
        }
    }
])




// change the following  sql query to mongodb
// select product_name, sum(quantity) as total_quantity
// from products
// join order_items on products.id = order_items.product_id
// join suppliers on products.supplier_id = suppliers.id
// where suppliers.name = ‘Maurice Wallace1715’
// group by product_name
// order by total_quantity desc
// limit 1; 

// write the mongodb query here
db.products.aggregate([
    {
        $lookup: {
            from: "order_items",
            localField: "id",
            foreignField: "product_id",
            as: "order_items"
        }
    },
    {
        $unwind: "$order_items"
    },
    {
        $lookup: {
            from: "suppliers",
            localField: "supplier_id",
            foreignField: "id",
            as: "suppliers"
        }
    },
    {
        $unwind: "$suppliers"
    },
    {
        $match: {
            "suppliers.name": "Maurice Wallace1715"
        }
    },
    {
        $group: {
            _id: "$product_name",
            total_quantity: {
                $sum: "$order_items.quantity"
            }
        }
    },
    {
        $sort: {
            total_quantity: -1
        }
    },
    {
        $limit: 1
    }
])



// change the following  sql query to mongodb
// select count(customer_order.id) as customers_count
// from (
// select count(customers.id) as total_record, customers.id, customers.name
// from customers
// join orders on orders.customer_id = customers.id
// join order_items on order_items.order_id = orders.id
// where age>25 and age<35
// group by customers.id
// having count(DISTINCT orders.id) >= 2
// and count(order_items.product_id) >= 3) customer_order;


// write the mongodb query here
db.customers.aggregate([
    {
        $lookup: {
            from: "orders",
            localField: "id",
            foreignField: "customer_id",
            as: "orders"
        }
    },
    {
        $unwind: "$orders"
    },
    {
        $lookup: {
            from: "order_items",
            localField: "orders.id",
            foreignField: "order_id",

            as: "order_items"
        }
    },
    {
        $unwind: "$order_items"
    },
    {
        $match: {
            age: {
                $gt: 25,
                $lt: 35
            }
        }
    },
    {
        $group: {
            _id: "$id",
            total_record: {
                $sum: 1
            },
            orders: {
                $addToSet: "$orders.id"
            },
            order_items: {

                $addToSet: "$order_items.product_id"
            }
        }
    },
    {
        $match: {
            total_record: {
                $gte: 2
            },
            orders: {
                $size: 2
            },
            order_items: {

                $size: 3
            }
        }
    },
    {
        $count: "customers_count"
    }
])




