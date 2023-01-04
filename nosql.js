// Q1
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



// Q2:
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





//Q3:
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






// Q4:
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




