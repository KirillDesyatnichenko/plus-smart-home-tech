CREATE TABLE IF NOT EXISTS warehouse_products (
    product_id UUID PRIMARY KEY,
    quantity BIGINT NOT NULL,
    fragile BOOLEAN,
    width DOUBLE PRECISION NOT NULL,
    height DOUBLE PRECISION NOT NULL,
    depth DOUBLE PRECISION NOT NULL,
    weight DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS order_bookings (
    booking_id UUID PRIMARY KEY,
    order_id UUID UNIQUE NOT NULL,
    delivery_id UUID
);

CREATE TABLE IF NOT EXISTS order_booking_products (
    booking_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (booking_id, product_id),
    CONSTRAINT fk_order_booking FOREIGN KEY (booking_id) REFERENCES order_bookings (booking_id)
);