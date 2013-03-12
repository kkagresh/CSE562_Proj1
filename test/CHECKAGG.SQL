CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT SUM(A) FROM R;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT AVG(A) FROM R;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT MIN(A) FROM R;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT MAX(A) FROM R;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT COUNT(*) FROM R;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/r.dat' USING CSV;

SELECT COUNT(*) FROM R, S;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT COUNT(*) FROM R, S WHERE R.B = S.B;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A) FROM R, S WHERE R.B = S.B;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A), AVG(C) FROM R, S WHERE R.B = S.B;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A*C) FROM R, S WHERE R.B = S.B;

CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T (C int, D int) FROM FILE 'test/t.dat' USING CSV;

SELECT SUM(A*D) FROM R, S, T WHERE (R.B = S.B) AND (S.C = T.C);