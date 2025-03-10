--
-- PostgreSQL database dump
--

-- Dumped from database version 14.15 (Homebrew)
-- Dumped by pg_dump version 17.0

-- Started on 2025-03-10 16:27:45 PDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 4 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- TOC entry 832 (class 1247 OID 57015)
-- Name: EmailType; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public."EmailType" AS ENUM (
    'MAIN',
    'CC'
);


--
-- TOC entry 835 (class 1247 OID 57020)
-- Name: OrderStatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public."OrderStatus" AS ENUM (
    'OPEN',
    'CLOSED',
    'VOID',
    'PENDING'
);


--
-- TOC entry 838 (class 1247 OID 57030)
-- Name: PaymentStatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public."PaymentStatus" AS ENUM (
    'UNPAID',
    'PAID',
    'PARTIAL',
    'PENDING',
    'FAILED'
);


--
-- TOC entry 841 (class 1247 OID 57042)
-- Name: PhoneType; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public."PhoneType" AS ENUM (
    'MAIN',
    'MOBILE',
    'WORK',
    'OTHER'
);


SET default_table_access_method = heap;

--
-- TOC entry 209 (class 1259 OID 57051)
-- Name: Address; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Address" (
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
-- TOC entry 210 (class 1259 OID 57056)
-- Name: Company; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Company" (
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
-- TOC entry 211 (class 1259 OID 57062)
-- Name: Customer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Customer" (
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
-- TOC entry 212 (class 1259 OID 57067)
-- Name: Order; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Order" (
    id text NOT NULL,
    "orderNumber" text NOT NULL,
    "orderDate" timestamp(3) without time zone NOT NULL,
    "customerId" text NOT NULL,
    "billingAddressId" text,
    "shippingAddressId" text,
    status public."OrderStatus" NOT NULL,
    "paymentStatus" public."PaymentStatus" NOT NULL,
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
-- TOC entry 213 (class 1259 OID 57072)
-- Name: CompanyStats; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public."CompanyStats" AS
 SELECT c.id,
    count(DISTINCT cust.id) AS "customerCount",
    COALESCE(sum(o."totalAmount"), (0)::numeric) AS "totalOrders"
   FROM ((public."Company" c
     LEFT JOIN public."Customer" cust ON ((cust."companyDomain" = c.domain)))
     LEFT JOIN public."Order" o ON ((o."customerId" = cust.id)))
  GROUP BY c.id;


--
-- TOC entry 214 (class 1259 OID 57077)
-- Name: CustomerEmail; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."CustomerEmail" (
    id text NOT NULL,
    email text NOT NULL,
    type public."EmailType" NOT NULL,
    "isPrimary" boolean DEFAULT false NOT NULL,
    "customerId" text NOT NULL
);


--
-- TOC entry 215 (class 1259 OID 57083)
-- Name: CustomerPhone; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."CustomerPhone" (
    id text NOT NULL,
    phone text NOT NULL,
    type public."PhoneType" NOT NULL,
    "isPrimary" boolean DEFAULT false NOT NULL,
    "customerId" text NOT NULL
);


--
-- TOC entry 216 (class 1259 OID 57089)
-- Name: OrderItem; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."OrderItem" (
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
-- TOC entry 217 (class 1259 OID 57094)
-- Name: Product; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Product" (
    id text NOT NULL,
    "productCode" text NOT NULL,
    name text NOT NULL,
    description text,
    "createdAt" timestamp(3) without time zone NOT NULL,
    "modifiedAt" timestamp(3) without time zone NOT NULL,
    cost numeric(10,2),
    "listPrice" numeric(10,2),
    "unitsPerPackage" integer DEFAULT 6 NOT NULL
);


--
-- TOC entry 219 (class 1259 OID 58020)
-- Name: ProductPriceHistory; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."ProductPriceHistory" (
    id text NOT NULL,
    "productId" text NOT NULL,
    cost numeric(10,2) NOT NULL,
    "listPrice" numeric(10,2) NOT NULL,
    "effectiveDate" timestamp(3) without time zone NOT NULL,
    notes text
);


--
-- TOC entry 218 (class 1259 OID 57099)
-- Name: _prisma_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._prisma_migrations (
    id character varying(36) NOT NULL,
    checksum character varying(64) NOT NULL,
    finished_at timestamp with time zone,
    migration_name character varying(255) NOT NULL,
    logs text,
    rolled_back_at timestamp with time zone,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    applied_steps_count integer DEFAULT 0 NOT NULL
);


--
-- TOC entry 3510 (class 2606 OID 57186)
-- Name: Address Address_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Address"
    ADD CONSTRAINT "Address_pkey" PRIMARY KEY (id);


--
-- TOC entry 3513 (class 2606 OID 57188)
-- Name: Company Company_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Company"
    ADD CONSTRAINT "Company_pkey" PRIMARY KEY (id);


--
-- TOC entry 3533 (class 2606 OID 57190)
-- Name: CustomerEmail CustomerEmail_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."CustomerEmail"
    ADD CONSTRAINT "CustomerEmail_pkey" PRIMARY KEY (id);


--
-- TOC entry 3537 (class 2606 OID 57192)
-- Name: CustomerPhone CustomerPhone_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."CustomerPhone"
    ADD CONSTRAINT "CustomerPhone_pkey" PRIMARY KEY (id);


--
-- TOC entry 3518 (class 2606 OID 57194)
-- Name: Customer Customer_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Customer"
    ADD CONSTRAINT "Customer_pkey" PRIMARY KEY (id);


--
-- TOC entry 3540 (class 2606 OID 57196)
-- Name: OrderItem OrderItem_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."OrderItem"
    ADD CONSTRAINT "OrderItem_pkey" PRIMARY KEY (id);


--
-- TOC entry 3526 (class 2606 OID 57198)
-- Name: Order Order_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_pkey" PRIMARY KEY (id);


--
-- TOC entry 3550 (class 2606 OID 58026)
-- Name: ProductPriceHistory ProductPriceHistory_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."ProductPriceHistory"
    ADD CONSTRAINT "ProductPriceHistory_pkey" PRIMARY KEY (id);


--
-- TOC entry 3544 (class 2606 OID 57200)
-- Name: Product Product_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Product"
    ADD CONSTRAINT "Product_pkey" PRIMARY KEY (id);


--
-- TOC entry 3547 (class 2606 OID 57202)
-- Name: _prisma_migrations _prisma_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._prisma_migrations
    ADD CONSTRAINT _prisma_migrations_pkey PRIMARY KEY (id);


--
-- TOC entry 3511 (class 1259 OID 57203)
-- Name: Company_domain_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Company_domain_key" ON public."Company" USING btree (domain);


--
-- TOC entry 3530 (class 1259 OID 57204)
-- Name: CustomerEmail_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerEmail_customerId_idx" ON public."CustomerEmail" USING btree ("customerId");


--
-- TOC entry 3531 (class 1259 OID 57205)
-- Name: CustomerEmail_email_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerEmail_email_idx" ON public."CustomerEmail" USING btree (email);


--
-- TOC entry 3534 (class 1259 OID 57206)
-- Name: CustomerPhone_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerPhone_customerId_idx" ON public."CustomerPhone" USING btree ("customerId");


--
-- TOC entry 3535 (class 1259 OID 57207)
-- Name: CustomerPhone_phone_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "CustomerPhone_phone_idx" ON public."CustomerPhone" USING btree (phone);


--
-- TOC entry 3514 (class 1259 OID 57208)
-- Name: Customer_billingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_billingAddressId_idx" ON public."Customer" USING btree ("billingAddressId");


--
-- TOC entry 3515 (class 1259 OID 57209)
-- Name: Customer_companyDomain_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_companyDomain_idx" ON public."Customer" USING btree ("companyDomain");


--
-- TOC entry 3516 (class 1259 OID 57210)
-- Name: Customer_customerName_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_customerName_idx" ON public."Customer" USING btree ("customerName");


--
-- TOC entry 3519 (class 1259 OID 57211)
-- Name: Customer_quickbooksId_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Customer_quickbooksId_key" ON public."Customer" USING btree ("quickbooksId");


--
-- TOC entry 3520 (class 1259 OID 57212)
-- Name: Customer_shippingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Customer_shippingAddressId_idx" ON public."Customer" USING btree ("shippingAddressId");


--
-- TOC entry 3538 (class 1259 OID 57213)
-- Name: OrderItem_orderId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_orderId_idx" ON public."OrderItem" USING btree ("orderId");


--
-- TOC entry 3541 (class 1259 OID 57214)
-- Name: OrderItem_productCode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_productCode_idx" ON public."OrderItem" USING btree ("productCode");


--
-- TOC entry 3542 (class 1259 OID 57215)
-- Name: OrderItem_serviceDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "OrderItem_serviceDate_idx" ON public."OrderItem" USING btree ("serviceDate");


--
-- TOC entry 3521 (class 1259 OID 57216)
-- Name: Order_billingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_billingAddressId_idx" ON public."Order" USING btree ("billingAddressId");


--
-- TOC entry 3522 (class 1259 OID 57217)
-- Name: Order_customerId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_customerId_idx" ON public."Order" USING btree ("customerId");


--
-- TOC entry 3523 (class 1259 OID 57218)
-- Name: Order_orderDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_orderDate_idx" ON public."Order" USING btree ("orderDate");


--
-- TOC entry 3524 (class 1259 OID 57219)
-- Name: Order_orderNumber_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Order_orderNumber_key" ON public."Order" USING btree ("orderNumber");


--
-- TOC entry 3527 (class 1259 OID 57220)
-- Name: Order_quickbooksId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_quickbooksId_idx" ON public."Order" USING btree ("quickbooksId");


--
-- TOC entry 3528 (class 1259 OID 57221)
-- Name: Order_shippingAddressId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_shippingAddressId_idx" ON public."Order" USING btree ("shippingAddressId");


--
-- TOC entry 3529 (class 1259 OID 57222)
-- Name: Order_status_paymentStatus_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Order_status_paymentStatus_idx" ON public."Order" USING btree (status, "paymentStatus");


--
-- TOC entry 3548 (class 1259 OID 58028)
-- Name: ProductPriceHistory_effectiveDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "ProductPriceHistory_effectiveDate_idx" ON public."ProductPriceHistory" USING btree ("effectiveDate");


--
-- TOC entry 3551 (class 1259 OID 58027)
-- Name: ProductPriceHistory_productId_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "ProductPriceHistory_productId_idx" ON public."ProductPriceHistory" USING btree ("productId");


--
-- TOC entry 3545 (class 1259 OID 57223)
-- Name: Product_productCode_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Product_productCode_key" ON public."Product" USING btree ("productCode");


--
-- TOC entry 3558 (class 2606 OID 57224)
-- Name: CustomerEmail CustomerEmail_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."CustomerEmail"
    ADD CONSTRAINT "CustomerEmail_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES public."Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3559 (class 2606 OID 57229)
-- Name: CustomerPhone CustomerPhone_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."CustomerPhone"
    ADD CONSTRAINT "CustomerPhone_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES public."Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3552 (class 2606 OID 57234)
-- Name: Customer Customer_billingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Customer"
    ADD CONSTRAINT "Customer_billingAddressId_fkey" FOREIGN KEY ("billingAddressId") REFERENCES public."Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3553 (class 2606 OID 57239)
-- Name: Customer Customer_companyDomain_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Customer"
    ADD CONSTRAINT "Customer_companyDomain_fkey" FOREIGN KEY ("companyDomain") REFERENCES public."Company"(domain) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3554 (class 2606 OID 57244)
-- Name: Customer Customer_shippingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Customer"
    ADD CONSTRAINT "Customer_shippingAddressId_fkey" FOREIGN KEY ("shippingAddressId") REFERENCES public."Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3560 (class 2606 OID 57249)
-- Name: OrderItem OrderItem_orderId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."OrderItem"
    ADD CONSTRAINT "OrderItem_orderId_fkey" FOREIGN KEY ("orderId") REFERENCES public."Order"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3561 (class 2606 OID 57254)
-- Name: OrderItem OrderItem_productCode_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."OrderItem"
    ADD CONSTRAINT "OrderItem_productCode_fkey" FOREIGN KEY ("productCode") REFERENCES public."Product"("productCode") ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3555 (class 2606 OID 57259)
-- Name: Order Order_billingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_billingAddressId_fkey" FOREIGN KEY ("billingAddressId") REFERENCES public."Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3556 (class 2606 OID 57264)
-- Name: Order Order_customerId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES public."Customer"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 3557 (class 2606 OID 57269)
-- Name: Order Order_shippingAddressId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_shippingAddressId_fkey" FOREIGN KEY ("shippingAddressId") REFERENCES public."Address"(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 3562 (class 2606 OID 58029)
-- Name: ProductPriceHistory ProductPriceHistory_productId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."ProductPriceHistory"
    ADD CONSTRAINT "ProductPriceHistory_productId_fkey" FOREIGN KEY ("productId") REFERENCES public."Product"(id) ON UPDATE CASCADE ON DELETE RESTRICT;


-- Completed on 2025-03-10 16:27:45 PDT

--
-- PostgreSQL database dump complete
--

