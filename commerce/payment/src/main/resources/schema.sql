CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID,
    product_cost DOUBLE PRECISION,
    delivery_cost DOUBLE PRECISION,
    fee_total DOUBLE PRECISION,
    total_payment DOUBLE PRECISION,
    status VARCHAR(20)
);