create table service_updates (
    update_id UUID PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    rid varchar(30) NOT NULL,
    uid varchar(10) NOT NULL,
    passenger BOOLEAN
);
create table locations (
    update_id UUID PRIMARY KEY,
    service_update_id UUID,
    time TIME NOT NULL,
    tpl varchar(10) NOT NULL,
    type varchar(10) NOT NULL,
    time_type varchar(10) NOT NULL,
    CONSTRAINT service_updates
        FOREIGN KEY(service_update_id) 
        REFERENCES service_updates(update_id)
);