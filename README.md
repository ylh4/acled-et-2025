# Conflict in Ethiopia: 2025 in Review

## ACLED-Only Quantitative Analysis Pipeline

This repository contains a fully reproducible, quantitative analysis pipeline for analyzing conflict in Ethiopia during 2025 using ACLED (Armed Conflict Location & Event Data Project) data.

## Project Overview

**Goal**: Build a comprehensive, ACLED-only quantitative analysis pipeline for "Conflict in Ethiopia: 2025 in Review" that produces publication-ready outputs suitable for a polished blog post.

## Methodology

The pipeline implements the following analytical approaches:

1. **Descriptive Statistics**: Events, fatalities, actors, event types, and spatial distribution
2. **Spatio-Temporal Modeling**: Negative binomial and zero-inflated count models
3. **Hotspot Mapping**: Spatial analysis using GeoPandas for conflict intensity visualization
4. **Historical Trend Comparisons**: Contextualizing 2025 within Ethiopia's long-term conflict trajectory (1997-2025)

## Core Components

- **ACLED API Ingestion**: Token-based OAuth2 authentication and data retrieval
- **Data Cleaning & Standardization**: Rigorous schema harmonization and quality assurance
- **Descriptive Analysis**: National totals, monthly/quarterly trends, actor breakdowns, regional summaries
- **Spatial Analysis**: Admin-level aggregation, choropleth mapping, hotspot detection
- **Statistical Modeling**: Panel data models, zero-inflated count models, optional Bayesian extensions
- **Output Generation**: Tables, figures, and summaries ready for publication

## Directory Structure

```
acled_et_2025/
├── data_raw/          # Raw ACLED API downloads
├── data_clean/        # Cleaned and standardized datasets
├── notebooks/         # Jupyter notebooks for exploratory analysis
├── src/              # Python modules and scripts
├── models/           # Trained model artifacts
├── figures/          # Generated visualizations
└── config/           # Configuration files and credentials
```

## Tools & Libraries

### Core Dependencies
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **requests**: HTTP requests for ACLED API
- **python-dotenv**: Environment variable management
- **geopandas**: Spatial data operations
- **statsmodels**: Statistical modeling (negative binomial, zero-inflated models)
- **scikit-learn**: Machine learning utilities

### Visualization
- **matplotlib**: Static plotting
- **seaborn**: Statistical visualizations
- **plotly**: Interactive charts

### Optional Advanced
- **pymc**: Bayesian modeling (PyMC)
- **arviz**: Bayesian diagnostics and visualization
- **contextily**: Basemap tiles for spatial plots

## Implementation Status

This pipeline is implemented step-by-step following the implementation manual:

- [x] Step 0: Purpose & Overall Architecture
- [ ] Step 1: Project Bootstrap (Repository & Environment Setup)
- [ ] Step 2: ACLED API Access (Configuration & Client Development)
- [ ] Step 3: Data Download for Ethiopia (1997-2025)
- [ ] Step 4: Data Cleaning & Harmonization
- [ ] Step 5: Descriptive Analysis for 2025
- [ ] Step 6: Spatial Analysis & Hotspot Detection
- [ ] Step 7: Spatio-Temporal & Zero-Inflated Modeling
- [ ] Step 8: Historical Comparison (Pre-2025 vs 2025)
- [ ] Step 9: Output Packaging for the Blog
- [ ] Step 10: Minimal Automation (Optional)
- [ ] Step 11: Documentation & Reproducibility

## Getting Started

1. **Set up environment** (see Step 1):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure ACLED credentials** (see Step 2):
   - Create `config/.env` with your ACLED credentials
   - See `config/.env.example` for template

3. **Run the pipeline**:
   - Follow steps sequentially or use the orchestration script (Step 10)

## Data Sources

- **ACLED (Armed Conflict Location & Event Data Project)**: Primary data source
  - Coverage: Ethiopia, 1997-2025
  - Access: OAuth2 authenticated API
  - Website: https://acleddata.com

## Outputs

All analysis outputs are organized in:
- `figures/`: Publication-ready visualizations
- `data_clean/`: Analysis-ready datasets
- `models/`: Trained statistical models
- Final export bundle for blog integration (Step 9)

## License

[Specify license as needed]

## Contact

[Add contact information as needed]

