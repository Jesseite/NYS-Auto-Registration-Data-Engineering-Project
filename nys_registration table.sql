CREATE TABLE IF NOT EXISTS public.nys_registration
(
    vin varchar PRIMARY KEY,
    city varchar(40),
    state varchar(30),
    zip int,
    county varchar(40),
    model_year int,
    make varchar(40),
    body_type varchar(40),
    fuel_type varchar(40),
    unladen_weight int,
    reg_valid_date date,
    reg_expiration_date date,
    color varchar(40)
)
