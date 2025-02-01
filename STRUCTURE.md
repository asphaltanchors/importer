/
│
├── pyproject.toml           # Project metadata and dependencies
├── Dockerfile              # Container definition
├── README.md               # Project documentation
│
├── csv_importer/           # Main package directory
│   ├── __init__.py        
│   ├── cli.py             # Command line interface
│   ├── config.py          # Configuration management
│   ├── importer.py        # Main import logic
│   ├── db/
│   │   ├── __init__.py
│   │   ├── models.py      # SQLAlchemy models
│   │   └── session.py     # Database session management
│   │
│   ├── processors/        # CSV processing logic
│   │   ├── __init__.py
│   │   ├── base.py       # Abstract base processor
│   │   └── parsers.py    # Specific CSV parsers
│   │
│   └── utils/
│       ├── __init__.py
│       ├── logging.py     # Logging configuration
│       └── validation.py  # Data validation helpers
│
├── tests/                 # Test directory
│   ├── __init__.py
│   ├── conftest.py       # pytest configuration
│   ├── test_importer.py
│   ├── test_processors.py
│   └── fixtures/         # Test data
│       └── sample.csv
│
└── scripts/              # Utility scripts
    ├── run_import.sh     # Shell script for cron
    ├── startup.sh       # Container startup script
    └── setup_db.py      # Database initialization
