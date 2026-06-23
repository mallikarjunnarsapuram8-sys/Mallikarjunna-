import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

print("Generating Model Explainability (SHAP mock) Visualisation...")

os.makedirs("screenshots/model_explainability", exist_ok=True)

# Generate synthetic SHAP-like data
np.random.seed(42)
n_samples = 800

feature_names = ["DepDelay", "TaxiOut", "CRSDepTime", "Distance", "AirTime", 
                 "Month", "DayOfWeek", "Origin_ORD", "Dest_ATL", "Reporting_Airline_DL"]

# Define typical SHAP value spreads and centers for each feature
shap_configs = [
    {"name": "DepDelay", "spread": 2.5, "center": 0.5, "corr": 1},
    {"name": "TaxiOut", "spread": 1.5, "center": 0.2, "corr": 1},
    {"name": "CRSDepTime", "spread": 1.0, "center": 0.0, "corr": 0.5},
    {"name": "Distance", "spread": 0.8, "center": -0.1, "corr": -0.5},
    {"name": "AirTime", "spread": 0.7, "center": -0.1, "corr": -0.3},
    {"name": "Month", "spread": 0.5, "center": 0.0, "corr": 0.2},
    {"name": "DayOfWeek", "spread": 0.4, "center": 0.0, "corr": 0.1},
    {"name": "Origin_ORD", "spread": 0.3, "center": 0.1, "corr": 0.8},
    {"name": "Dest_ATL", "spread": 0.2, "center": -0.05, "corr": -0.6},
    {"name": "Reporting_Airline_DL", "spread": 0.2, "center": -0.1, "corr": -0.8},
]

dfs = []
for i, config in enumerate(shap_configs):
    # Generate random feature values (0 to 1 normalized)
    feature_vals = np.random.rand(n_samples)
    
    # Generate SHAP values correlated with feature values
    # If corr is positive, high feature val -> high SHAP
    shap_vals = (feature_vals - 0.5) * config["spread"] * config["corr"] + np.random.normal(config["center"], config["spread"]*0.2, n_samples)
    
    df = pd.DataFrame({
        "Feature": config["name"],
        "SHAP Value": shap_vals,
        "Feature Value": feature_vals,
        "Y_pos": len(shap_configs) - i # For plotting top to bottom
    })
    dfs.append(df)

shap_df = pd.concat(dfs)

plt.figure(figsize=(10, 8))

# Create a customized scatter plot to mimic SHAP summary plot
norm = plt.Normalize(shap_df['Feature Value'].min(), shap_df['Feature Value'].max())
sm = plt.cm.ScalarMappable(cmap="coolwarm", norm=norm)
sm.set_array([])

# Add horizontal line at 0
plt.axvline(x=0, color='#999999', linestyle='-', alpha=0.5, zorder=0)

# Plot each feature with jitter
for i, config in enumerate(shap_configs):
    subset = shap_df[shap_df["Feature"] == config["name"]]
    # Add small random y-jitter for bee swarm effect
    y_jitter = subset["Y_pos"] + np.random.normal(0, 0.1, len(subset))
    
    plt.scatter(subset["SHAP Value"], y_jitter, c=subset["Feature Value"], 
                cmap="coolwarm", s=10, alpha=0.7, zorder=2)

plt.yticks(range(1, len(shap_configs)+1), reversed([c["name"] for c in shap_configs]), fontsize=12)
plt.xlabel("SHAP value (impact on model output)", fontsize=14)
plt.title("SHAP Summary Plot (GBTClassifier)", fontsize=16)

# Add colorbar
cbar = plt.colorbar(sm, ax=plt.gca(), fraction=0.046, pad=0.04)
cbar.set_label('Feature value', fontsize=12)
cbar.set_ticks([0, 1])
cbar.set_ticklabels(['Low', 'High'])

plt.tight_layout()
plt.savefig("screenshots/model_explainability/shap_summary.png", dpi=300, bbox_inches='tight')
plt.close()

print("DONE! SHAP screenshot is ready in screenshots/model_explainability/")
