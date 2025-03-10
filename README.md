# Biomarkers
BioMarkers Based Disease Risk Prediction System
This repository contains the implementation of a Multi-Agent System for disease risk prediction using biomarkers. The project was developed for the **All of Us Hackathon 2025**, focusing on predicting the risk of diabetes, cardiovascular disease, and Alzheimer's based on patient biomarker trends.

By leveraging machine learning, and multi-agent systems, our model provides personalized health risk assessments based on biomedical data extracted from All of Us Workbench.

**Dataset Details**
Total Records: ~19M  records with biomarker measurements
Unique Patients: ~13000
Data Source: Extracted from All of Us Workbench (BigQuery)
Time Period: Longitudinal patient biomarker data over multiple years

**Technologies Used**

Data Processing & Storage: **BigQuery, Pandas, NumPy**

Computing Environment: Jupyter Notebook (All of Us Workbench)

Machine Learning Frameworks: Scikit-learn, LightGBM, TensorFlow

Feature Engineering & Analysis: SHAP (Explainable AI), Autoencoders

Visualization: Matplotlib, Seaborn, Plotly

Security & Compliance: HIPAA-compliant environment, All of Us Workbench
![image](https://github.com/user-attachments/assets/6d0fbe38-5961-4152-af16-4d909895b13c)



**Advanced Data Cleaning & Processing Techniques**

**Dynamic Outlier Detection:**
Used Isolation Forest and Z-score filtering to remove outliers dynamically based on feature distributions.

**Adaptive Imputation for Missing Values:**
Implemented K-Nearest Neighbors (KNN) Imputation to predict missing biomarker values instead of simple mean/mode imputation.
Time-Series Feature Engineering:
Created biomarker change rate features to analyze fluctuations over time.
Used Fourier Transforms to extract periodic trends from longitudinal biomarker data.
Anomaly Detection for Disease Screening:
Applied Autoencoders and One-Class SVM to detect rare biomarker patterns linked to early disease onset.


**Machine Learning Algorithms Used (Advanced)**

**LightGBM**(Gradient Boosting Trees): Faster and more efficient than XGBoost for handling large datasets with categorical features.

**Contrastive Learning for Biomarker Trends**: Implemented Siamese Networks to learn similarities between patients with early disease markers.

**Federated Learning for Privacy-Preserving AI**: Used TensorFlow Federated (TFF) to train models without sharing raw patient data.

**SHAP & Counterfactual Explainability**: Applied Counterfactual AI models to determine how biomarker values should change for lower disease risk.

**Streamlit Web App for Disease Risk Prediction**
**Frontend**: Developed an interactive and user-friendly interface using Streamlit.
**User Input**: Patients can enter biomarker values such as HbA1c, LDL, Systolic BP, along with age and medical history.
**Risk Score Calculation**: AI model predicts the probability of disease using the biomarker values entered by the user.

**Key Innovations & Contributions**

1. Multi-Agent AI for Risk Prediction
Developed a multi-agent system, where specialized AI models predict risks for diabetes, cardiovascular disease, and Alzheimer's.
Each agent fine-tunes its model based on patient demographics, medication history, and biomarker fluctuations.
Improved model generalization across diverse patient populations without compromising data security.

2. Biomarker-Based Clustering for Early Diagnosis
Identified high-risk subgroups using unsupervised learning, improving early detection and treatment planning.


**Demo Link**: 
https://youtu.be/uE8LQeD8VHs?si=nZHtJ9M4G0iz366i
