# AI Agent Instructions for database_PX

## Project Overview
This is the Peixonaut database project, designed to manage, visualize, and analyze data. The project is organized into three main components:

1. Data Pulling (`scipts to pull data/`)
   - Purpose: Scripts for fetching and ingesting data from various sources
   - Key operations: Data extraction and transformation

2. Data Visualization (`script to visualize data/`)
   - Purpose: Tools and scripts for creating visual representations of the data
   - Expected outputs: Charts, graphs, and other visual analytics

3. Data Storage (`scripts to store data/`)
   - Purpose: Scripts for managing persistent data storage
   - Responsibilities: Database operations, data persistence, and retrieval

## Project Structure
```
.
├── scipts to pull data/     # Data ingestion scripts
├── script to visualize data/ # Visualization tools
└── scripts to store data/   # Data storage management
```

## Development Guidelines
1. Script Organization
   - Keep data-related scripts in their respective functional directories
   - Name scripts descriptively to indicate their primary purpose

2. Data Flow
   - Pull data → Store data → Visualize data
   - Ensure data transformations are documented inline

## Areas for Contribution
- Data ingestion scripts in `scipts to pull data/`
- Visualization tools in `script to visualize data/`
- Storage mechanisms in `scripts to store data/`

## Note for AI Agents
This project is in its initial setup phase. When contributing:
1. Follow the established directory structure
2. Document data flows and transformations
3. Consider data validation and error handling
4. Focus on maintainable and well-documented code

## Getting Started
1. Review the project structure
2. Identify which component you'll be working with (pull, store, or visualize)
3. Follow the corresponding directory conventions