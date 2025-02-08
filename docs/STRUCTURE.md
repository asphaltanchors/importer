/
│
├── pyproject.toml          # Project metadata and dependencies
├── Dockerfile             # Container definition
├── README.md              # Project documentation
│
├── importer/             # Main package directory
│   ├── __init__.py
│   ├── importer.py       # Core import functionality
│   ├── utils.py          # Common utilities
│   │
│   ├── cli/             # Command line interface
│   │   ├── __init__.py
│   │   ├── __main__.py  # CLI entry point
│   │   ├── base.py      # Base command classes
│   │   ├── config.py    # Configuration management
│   │   ├── logging.py   # Logging configuration
│   │   └── main.py      # Command registration
│   │
│   ├── commands/        # Command implementations
│   │   ├── __init__.py
│   │   ├── customers/   # Customer-related commands
│   │   ├── sales/       # Sales-related commands
│   │   ├── validate/    # Validation commands
│   │   └── verify/      # Verification commands
│   │
│   ├── db/             # Database layer
│   │   ├── __init__.py
│   │   ├── session.py  # Session management
│   │   └── models/     # SQLAlchemy models
│   │       ├── __init__.py
│   │       ├── base.py
│   │       ├── company.py
│   │       ├── customer.py
│   │       ├── order.py
│   │       └── product.py
│   │
│   ├── processors/     # Data processors
│   │   ├── __init__.py
│   │   ├── base.py    # Base processor class
│   │   ├── company.py # Company processing
│   │   ├── customer.py # Customer processing
│   │   ├── product.py # Product processing
│   │   ├── invoice.py # Invoice processing
│   │   ├── sales_receipt.py # Receipt processing
│   │   ├── line_item.py # Line item processing
│   │   ├── error_tracker.py # Error tracking
│   │   └── validator.py # Data validation
│   │
│   └── utils/         # Utility modules
│       ├── __init__.py
│       ├── csv_normalization.py
│       ├── normalization.py
│       ├── product_mapping.py
│       └── system_products.py
│
├── tests/            # Test directory
│   ├── __init__.py
│   ├── test_customer_import.py
│   ├── test_invoice_import.py
│   └── test_domain_normalization.py
│
├── docs/            # Documentation
│   ├── CUSTOMER.md  # Customer processing docs
│   ├── NORMALIZE.md # Data normalization docs
│   ├── PLAN.md     # Project planning
│   ├── SALES.md    # Sales processing docs
│   ├── schema.sql  # Database schema
│   └── STRUCTURE.md # This file
│
└── scripts/        # Utility scripts
    ├── run_import.sh    # Import automation
    └── startup.sh      # Container startup
