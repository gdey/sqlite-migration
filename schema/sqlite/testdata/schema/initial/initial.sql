CREATE TABLE cases (
  case_number text NOT NULL PRIMARY KEY
, request_id integer NOT NULL
, scenario real NOT NULL
, next_request integer generated always as ( request_id + 1 ) stored
, past_request integer generated always as ( request_id - 1 ) virtual

, created_at timestamp -- managed via triggers
, updated_at timestamp -- managed via triggers
);

CREATE TRIGGER cases_created_at
    AFTER INSERT ON cases
BEGIN
    UPDATE cases SET created_at = datetime ('now'), updated_at = datetime ('now')
    WHERE rowid = NEW.rowid;
END;

CREATE TRIGGER cases_updated_at
    AFTER UPDATE ON cases
BEGIN
    UPDATE cases SET updated_at = datetime ('now')
    WHERE rowid = NEW.rowid;
END;
CREATE TABLE item_applications (
  code TEXT PRIMARY KEY
, name TEXT NOT NULL
);


INSERT INTO item_applications
( code, name )
VALUES
  ( 'G' , 'geology'                )
, ( 'M' , 'matching'                 )
, ( 'R' , 'research'                 )
, ( 'T' , 'analyst training/testing' )
, ( 'V' , 'validation'               )
, ( 'PC', 'performance check'        )
;

CREATE TABLE items (
 case_number TEXT NOT NULL
, item_number INTEGER NOT NULL
, agency_item_number TEXT NOT NULL
, may_consume boolean NOT NULL DEFAULT 0
, known boolean
, source TEXT NOT NULL ON CONFLICT REPLACE
, application TEXT NOT NULL ON CONFLICT REPLACE REFERENCES item_applications(code)

, created_at timestamp -- managed via triggers
, updated_at timestamp -- managed via triggers

, FOREIGN KEY (case_number) REFERENCES cases(case_number)
, PRIMARY KEY (case_number, item_number)
);

CREATE TRIGGER items_created_at
    AFTER INSERT ON items
BEGIN
    UPDATE items SET created_at = datetime ('now'), updated_at = datetime ('now')
    WHERE rowid = NEW.rowid;
END;

CREATE TRIGGER items_updated_at
    AFTER UPDATE ON items
BEGIN
    UPDATE items SET updated_at = datetime ('now')
    WHERE rowid = NEW.rowid;
END;
