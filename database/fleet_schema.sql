-- Fleet management database schema
-- Trip, driver, owner, and truck tables

DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS trucks;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS owners;

CREATE TABLE owners (
    owner_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trucks (
    truck_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    plate_number TEXT NOT NULL UNIQUE,
    vin TEXT NOT NULL UNIQUE,
    make TEXT,
    model TEXT,
    year INTEGER,
    capacity_tons REAL,
    current_status TEXT NOT NULL DEFAULT 'available',
    mileage REAL DEFAULT 0,
    last_service_date TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_truck_owner
        FOREIGN KEY (owner_id) REFERENCES owners(owner_id)
);

CREATE TABLE drivers (
    driver_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    license_number TEXT NOT NULL UNIQUE,
    license_class TEXT,
    phone TEXT,
    email TEXT,
    date_of_birth TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    hire_date TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_driver_owner
        FOREIGN KEY (owner_id) REFERENCES owners(owner_id)
);

CREATE TABLE trips (
    trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    truck_id INTEGER NOT NULL,
    driver_id INTEGER NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    cargo_type TEXT,
    cargo_weight REAL,
    distance_km REAL,
    trip_status TEXT NOT NULL DEFAULT 'scheduled',
    scheduled_start_time TEXT,
    actual_start_time TEXT,
    actual_end_time TEXT,
    fuel_cost REAL,
    revenue REAL,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trip_owner
        FOREIGN KEY (owner_id) REFERENCES owners(owner_id),
    CONSTRAINT fk_trip_truck
        FOREIGN KEY (truck_id) REFERENCES trucks(truck_id),
    CONSTRAINT fk_trip_driver
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE INDEX idx_trucks_owner_id ON trucks(owner_id);
CREATE INDEX idx_drivers_owner_id ON drivers(owner_id);
CREATE INDEX idx_trips_owner_id ON trips(owner_id);
CREATE INDEX idx_trips_truck_id ON trips(truck_id);
CREATE INDEX idx_trips_driver_id ON trips(driver_id);
CREATE INDEX idx_trips_status ON trips(trip_status);
CREATE INDEX idx_trips_start_time ON trips(actual_start_time);
CREATE INDEX idx_trucks_plate_number ON trucks(plate_number);
CREATE INDEX idx_drivers_license_number ON drivers(license_number);

INSERT INTO owners (company_name, contact_name, email, phone, address, status)
VALUES
    ('NorthStar Logistics', 'Alicia Reed', 'alicia@nortstarlogistics.com', '+1-555-0101', '123 Freight Ave, Dallas, TX', 'active'),
    ('BlueRoute Transport', 'Marcus Lee', 'marcus@blueroutetransport.com', '+1-555-0102', '88 Harbor Rd, Atlanta, GA', 'active');

INSERT INTO trucks (owner_id, plate_number, vin, make, model, year, capacity_tons, current_status, mileage, last_service_date)
VALUES
    (1, 'TX-4012', '1HGBH41JXMN109186', 'Volvo', 'VNL 760', 2022, 18.50, 'available', 245500.00, '2026-08-15'),
    (1, 'TX-4023', '3VWFE21C09M123456', 'Kenworth', 'T680', 2021, 20.00, 'in_transit', 268700.00, '2026-07-22'),
    (2, 'GA-5501', 'JH4KA2650LC012345', 'Freightliner', 'Cascadia', 2023, 22.00, 'available', 182000.00, '2026-09-01');

INSERT INTO drivers (owner_id, first_name, last_name, license_number, license_class, phone, email, date_of_birth, status, hire_date)
VALUES
    (1, 'Daniel', 'Baker', 'DLA-88921', 'Class A', '+1-555-2201', 'daniel.baker@example.com', '1990-03-14', 'active', '2021-05-10'),
    (1, 'Priya', 'Sharma', 'DLA-77432', 'Class A', '+1-555-2202', 'priya.sharma@example.com', '1988-11-05', 'active', '2020-08-19'),
    (2, 'Luis', 'Martinez', 'DLA-66114', 'Class A', '+1-555-2203', 'luis.martinez@example.com', '1992-01-25', 'active', '2022-02-11');

INSERT INTO trips (owner_id, truck_id, driver_id, origin, destination, cargo_type, cargo_weight, distance_km, trip_status, scheduled_start_time, actual_start_time, actual_end_time, fuel_cost, revenue, notes)
VALUES
    (1, 1, 1, 'Dallas, TX', 'Houston, TX', 'Food products', 12000.00, 390.00, 'completed', '2026-09-01 08:00:00', '2026-09-01 08:15:00', '2026-09-01 16:45:00', 420.00, 1850.00, 'On-time delivery'),
    (1, 2, 2, 'Austin, TX', 'Memphis, TN', 'Electronics', 9000.00, 610.00, 'in_transit', '2026-09-10 06:30:00', '2026-09-10 06:45:00', NULL, 520.00, 2600.00, 'Route check in progress'),
    (2, 3, 3, 'Atlanta, GA', 'Charlotte, NC', 'Medical supplies', 8000.00, 420.00, 'scheduled', '2026-09-12 07:00:00', NULL, NULL, 310.00, 1900.00, 'Awaiting departure');
