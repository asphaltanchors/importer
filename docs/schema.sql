--
-- PostgreSQL database dump
--

-- Dumped from database version 14.15 (Homebrew)
-- Dumped by pg_dump version 17.0

-- Started on 2025-02-07 21:20:30 PST

--
-- TOC entry 834 (class 1247 OID 41306)
-- Name: EmailType; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE "EmailType" AS ENUM (
    'MAIN',
    'CC'
);


--
-- TOC entry 840 (class 1247 OID 41328)
-- Name: OrderStatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE "OrderStatus" AS ENUM (
    'OPEN',
    'CLOSED',
    'VOID',
    'PENDING'
);


--
-- TOC entry 843 (class 1247 OID 41338)
-- Name: PaymentStatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE "PaymentStatus" AS ENUM (
    'UNPAID',
    'PAID',
    'PARTIAL',
    'PENDING',
    'FAILED'
);


--
-- TOC entry 837 (class 1247 OID 41312)
-- Name: PhoneType; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE "PhoneType" AS ENUM (
    'MAIN',
    'MOBILE',
    'WORK',
    'OTHER'
);


SET default_table_access_method = heap;

--
-- TOC entry 213 (class 1259 OID 41372)
-- Name: Address; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "Address" (
    id text NOT NULL,
    line1 text NOT NULL,
    line2 text,
    line3 text,
    city text NOT NULL,
    state text,
    "postalCode" text,
    country text
);


--
-- TOC entry 214 (class 1259 OID 41379)
-- Name: Company; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "Company" (
    id text NOT NULL,
    domain text NOT NULL,
    name text,
    enriched boolean DEFAULT false NOT NULL,
    "enrichedAt" timestamp(3) without time zone,
    "enrichedSource" text,
    "enrichmentData" jsonb,
    "createdAt" timestamp(3) without time zone,
    "enrichmentError" text
);


--
-- TOC entry 210 (class 1259 OID 41349)
-- Name: Customer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "Customer" (
    id text NOT NULL,
    "quickbooksId" text NOT NULL,
    "customerName" text NOT NULL,
    "companyDomain" text,
    "billingAddressId" text,
    "shippingAddressId" text,
    "taxCode" text,
    "taxItem" text,
    "resaleNumber" text,
    "creditLimit" numeric(65,30),
    terms text,
    status text NOT NULL,
    "createdAt" timestamp(3) without time zone NOT NULL,
    "modifiedAt" timestamp(3) without time zone NOT NULL,
    "sourceData" jsonb NOT NULL
);


--
-- TOC entry 216 (class 1259 OID 41394)
-- Name: Order; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "Order" (
    id text NOT NULL,
    "orderNumber" text NOT NULL,
    "orderDate" timestamp(3) without time zone NOT NULL,
    "customerId" text NOT NULL,
    "billingAddressId" text,
    "shippingAddressId" text,
    status "OrderStatus" NOT NULL,
    "paymentStatus" "PaymentStatus" NOT NULL,
    "paymentMethod" text,
    "paymentDate" timestamp(3) without time zone,
    "dueDate" timestamp(3) without time zone,
    terms text,
    subtotal numeric(65,30) NOT NULL,
    "taxAmount" numeric(65,30) NOT NULL,
    "taxPercent" numeric(65,30),
    "totalAmount" numeric(65,30) NOT NULL,
    "shipDate" timestamp(3) without time zone,
    "shippingMethod" text,
    "createdAt" timestamp(3) without time zone NOT NULL,
    "modifiedAt" timestamp(3) without time zone NOT NULL,
    "sourceData" jsonb NOT NULL,
    "poNumber" text,
    "quickbooksId" text,
    class text
);


--
-- TOC entry 218 (class 1259 OID 43307)
-- Name: CompanyStats; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW "CompanyStats" AS
 SELECT c.id,
    count(DISTINCT cust.id) AS "customerCount",
    COALESCE(sum(o."totalAmount"), (0)::numeric) AS "totalOrders"
   FROM (("Company" c
     LEFT JOIN "Customer" cust ON ((cust."companyDomain" = c.domain)))
     LEFT JOIN "Order" o ON ((o."customerId" = cust.id)))
  GROUP BY c.id;


--
-- TOC entry 211 (class 1259 OID 41356)
-- Name: CustomerEmail; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "CustomerEmail" (
    id text NOT NULL,
    email text NOT NULL,
    type "EmailType" NOT NULL,
    "isPrimary" boolean DEFAULT false NOT NULL,
    "customerId" text NOT NULL
);


--
-- TOC entry 212 (class 1259 OID 41364)
-- Name: CustomerPhone; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "CustomerPhone" (
    id text NOT NULL,
    phone text NOT NULL,
    type "PhoneType" NOT NULL,
    "isPrimary" boolean DEFAULT false NOT NULL,
    "customerId" text NOT NULL
);


--
-- TOC entry 217 (class 1259 OID 41401)
-- Name: OrderItem; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "OrderItem" (
    id text NOT NULL,
    "orderId" text NOT NULL,
    "productCode" text NOT NULL,
    description text,
    quantity numeric(65,30) NOT NULL,
    "unitPrice" numeric(65,30) NOT NULL,
    amount numeric(65,30) NOT NULL,
    "serviceDate" timestamp(3) without time zone,
    "sourceData" jsonb
);


--
-- TOC entry 215 (class 1259 OID 41387)
-- Name: Product; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE "Product" (
    id text NOT NULL,
    "productCode" text NOT NULL,
    name text NOT NULL,
    description text,
    "createdAt" timestamp(3) without time zone NOT NULL,
    "modifiedAt" timestamp(3) without time zone NOT NULL
);


--
-- TOC entry 3522 (class 2606 OID 41378)
-- Name: Address Address_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Address"
    ADD CONSTRAINT "Address_pkey" PRIMARY KEY (id);


--
-- TOC entry 3525 (class 2606 OID 41386)
-- Name: Company Company_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Company"
    ADD CONSTRAINT "Company_pkey" PRIMARY KEY (id);


--
-- TOC entry 3516 (class 2606 OID 41363)
-- Name: CustomerEmail CustomerEmail_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "CustomerEmail"
    ADD CONSTRAINT "CustomerEmail_pkey" PRIMARY KEY (id);


--
-- TOC entry 3520 (class 2606 OID 41371)
-- Name: CustomerPhone CustomerPhone_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "CustomerPhone"
    ADD CONSTRAINT "CustomerPhone_pkey" PRIMARY KEY (id);


--
-- TOC entry 3510 (class 2606 OID 41355)
-- Name: Customer Customer_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Customer"
    ADD CONSTRAINT "Customer_pkey" PRIMARY KEY (id);


--
-- TOC entry 3540 (class 2606 OID 41407)
-- Name: OrderItem OrderItem_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "OrderItem"
    ADD CONSTRAINT "OrderItem_pkey" PRIMARY KEY (id);


--
-- TOC entry 3534 (class 2606 OID 41400)
-- Name: Order Order_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Order"
    ADD CONSTRAINT "Order_pkey" PRIMARY KEY (id);


--
-- TOC entry 3527 (class 2606 OID 41393)
-- Name: Product Product_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Product"
    ADD CONSTRAINT "Product_pkey" PRIMARY KEY (id);


--
-- TOC entry 3505 (class 2606 OID 41304)
-- Name: _prisma_migrations _prisma_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

--
-- TOC entry 3523 (class 1259 OID 41417)
-- Name: Company_domain_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Company_domain_key" ON "Company" USING btree (domain);


--
-- TOC entry 3513 (class 1259 OID 41414)
-- Name: CustomerEmail_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerEmail_customerId_idx" ON "CustomerEmail" USING btree ("customerId");


--
-- TOC entry 3514 (class 1259 OID 41413)
-- Name: CustomerEmail_email_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerEmail_email_idx" ON "CustomerEmail" USING btree (email);


--
-- TOC entry 3517 (class 1259 OID 41416)
-- Name: CustomerPhone_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerPhone_customerId_idx" ON "CustomerPhone" USING btree ("customerId");


--
-- TOC entry 3518 (class 1259 OID 41415)
-- Name: CustomerPhone_phone_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerPhone_phone_idx" ON "CustomerPhone" USING btree (phone);


--
-- TOC entry 3506 (class 1259 OID 41479)
-- Name: Customer_billingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_billingAddressId_idx" ON "Customer" USING btree ("billingAddressId");


--
-- TOC entry 3507 (class 1259 OID 41412)
-- Name: Customer_companyDomain_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_companyDomain_idx" ON "Customer" USING btree ("companyDomain");


--
-- TOC entry 3508 (class 1259 OID 41411)
-- Name: Customer_customerName_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_customerName_idx" ON "Customer" USING btree ("customerName");


--
-- TOC entry 3511 (class 1259 OID 41408)
-- Name: Customer_quickbooksId_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Customer_quickbooksId_key" ON "Customer" USING btree ("quickbooksId");


--
-- TOC entry 3512 (class 1259 OID 41480)
-- Name: Customer_shippingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_shippingAddressId_idx" ON "Customer" USING btree ("shippingAddressId");


--
-- TOC entry 3538 (class 1259 OID 41426)
-- Name: OrderItem_orderId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_orderId_idx" ON "OrderItem" USING btree ("orderId");


--
-- TOC entry 3541 (class 1259 OID 41427)
-- Name: OrderItem_productCode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_productCode_idx" ON "OrderItem" USING btree ("productCode");


--
-- TOC entry 3542 (class 1259 OID 41428)
-- Name: OrderItem_serviceDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_serviceDate_idx" ON "OrderItem" USING btree ("serviceDate");


--
-- TOC entry 3529 (class 1259 OID 41481)
-- Name: Order_billingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_billingAddressId_idx" ON "Order" USING btree ("billingAddressId");


--
-- TOC entry 3530 (class 1259 OID 41422)
-- Name: Order_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_customerId_idx" ON "Order" USING btree ("customerId");


--
-- TOC entry 3531 (class 1259 OID 41423)
-- Name: Order_orderDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_orderDate_idx" ON "Order" USING btree ("orderDate");


--
-- TOC entry 3532 (class 1259 OID 41419)
-- Name: Order_orderNumber_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Order_orderNumber_key" ON "Order" USING btree ("orderNumber");


--
-- TOC entry 3535 (class 1259 OID 41425)
-- Name: Order_quickbooksId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_quickbooksId_idx" ON "Order" USING btree ("quickbooksId");


--
-- TOC entry 3536 (class 1259 OID 41482)
-- Name: Order_shippingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_shippingAddressId_idx" ON "Order" USING btree ("shippingAddressId");


--
-- TOC entry 3537 (class 1259 OID 41424)
-- Name: Order_status_paymentStatus_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_status_paymentStatus_idx" ON "Order" USING btree (status, "paymentStatus");


--
-- TOC entry 3528 (class 1259 OID 41418)
-- Name: Product_productCode_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Product_productCode_key" ON "Product" USING btree ("productCode");


--
-- TOC entry 3546 (class 2606 OID 41444)
-- Name: CustomerEmail CustomerEmail_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "CustomerEmail"
    ADD CONSTRAINT "CustomerEmail_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES "Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3547 (class 2606 OID 41449)
-- Name: CustomerPhone CustomerPhone_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "CustomerPhone"
    ADD CONSTRAINT "CustomerPhone_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES "Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3543 (class 2606 OID 41434)
-- Name: Customer Customer_billingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Customer"
    ADD CONSTRAINT "Customer_billingAddressId_fkey" FOREIGN KEY ("billingAddressId") REFERENCES "Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3544 (class 2606 OID 41429)
-- Name: Customer Customer_companyDomain_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Customer"
    ADD CONSTRAINT "Customer_companyDomain_fkey" FOREIGN KEY ("companyDomain") REFERENCES "Company"(domain) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3545 (class 2606 OID 41439)
-- Name: Customer Customer_shippingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Customer"
    ADD CONSTRAINT "Customer_shippingAddressId_fkey" FOREIGN KEY ("shippingAddressId") REFERENCES "Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3551 (class 2606 OID 41469)
-- Name: OrderItem OrderItem_orderId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "OrderItem"
    ADD CONSTRAINT "OrderItem_orderId_fkey" FOREIGN KEY ("orderId") REFERENCES "Order"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3552 (class 2606 OID 41474)
-- Name: OrderItem OrderItem_productCode_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "OrderItem"
    ADD CONSTRAINT "OrderItem_productCode_fkey" FOREIGN KEY ("productCode") REFERENCES "Product"("productCode") ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3548 (class 2606 OID 41459)
-- Name: Order Order_billingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Order"
    ADD CONSTRAINT "Order_billingAddressId_fkey" FOREIGN KEY ("billingAddressId") REFERENCES "Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3549 (class 2606 OID 41454)
-- Name: Order Order_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Order"
    ADD CONSTRAINT "Order_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES "Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3550 (class 2606 OID 41464)
-- Name: Order Order_shippingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY "Order"
    ADD CONSTRAINT "Order_shippingAddressId_fkey" FOREIGN KEY ("shippingAddressId") REFERENCES "Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


-- Completed on 2025-02-07 21:20:30 PST

--
-- PostgreSQL database dump complete
--

