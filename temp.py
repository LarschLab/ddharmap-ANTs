import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSV file
df = pd.read_csv("/Users/ddharmap/Library/CloudStorage/OneDrive-UniversitédeLausanne/Academic/PhD/Presentations/01 Conferences/retreatCIG/L395_f11_plane1_trace.csv")

# Check column names
print(df.head())

# Plot ΔF/F vs. frame
plt.figure(figsize=(10, 5))
plt.plot(df["Frame"], df["∆F/F"], linewidth=1, color="black")

# Labels
plt.xlabel("Frame")
plt.ylabel("ΔF/F")
plt.title("Calcium Trace (ΔF/F vs Frame)")

# Add raster grid
plt.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)

# Custom grid spacing
plt.gca().set_xticks(range(0, int(df["Frame"].max())+1, 500))
plt.gca().set_yticks(np.arange(0, 0.51, 0.1))
plt.ylim(-0.025, 0.5)

plt.tight_layout()
plt.show()
