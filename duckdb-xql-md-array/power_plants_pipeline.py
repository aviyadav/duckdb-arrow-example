#!/usr/bin/env python3

"""

Working reproduction of the slide "Act 1: Intro to SciPy - Pandas".



The slide snippet (Dask + a private `dask_ee` reader) is:



    df = dask_ee.read_ee(fc)

    (

        df[df.comm_year.gt(1940) & df.country.eq("USA") & df.fuel1.isin(["Coal", "Wind"])]

        .astype({"comm_year": int})

        .drop(columns=["geo"])

        .groupby(["comm_year", "fuel1"])

        .agg({"capacitymw": "sum"})

        .reset_index()

        .sort_values(by=["comm_year"])

        .compute(scheduler="threads")

        .pivot_table(index="comm_year", columns="fuel1", values="capacitymw", fill_value=0)

        .plot()

    )



This script:

  * fabricates a global power-plant table with the same columns as the slide,

  * provides a stand-in for `dask_ee.read_ee` (Dask if installed, else pandas),

  * runs the same method chain (`.compute()` is routed through a helper so the

    program also works without Dask installed),

  * saves the Coal-vs-Wind chart to `usa_capacity_coal_wind.png`.



Requirements: numpy, pandas, matplotlib   (optional: dask for the lazy path)

"""



import numpy as np

import pandas as pd

import matplotlib

import matplotlib.pyplot as plt



try:

    import dask.dataframe as dd

except ImportError:          # Dask is optional -> pure-pandas fallback

    dd = None



# ----------------------------------------------------------------------------------

# Fake source data (mirrors the column layout shown at the bottom of the slide)

# ----------------------------------------------------------------------------------

# code: (name, lat, lon, sampling weight)

COUNTRIES = {

    "USA": ("United States", 39.8, -98.6, 0.40),

    "MEX": ("Mexico",        23.6, -102.5, 0.12),

    "CAN": ("Canada",        56.1, -106.3, 0.10),

    "DEU": ("Germany",       51.2,  10.4,  0.10),

    "CHN": ("China",         35.9,  104.2, 0.13),

    "IND": ("India",         21.0,  78.0,  0.15),

}

# fuel: (mean comm_year, std, typical MW, sampling weight) -> shapes the plot like the slide

FUELS = {

    "Coal":    (1972, 12, 450, 0.22),

    "Wind":    (2012,  6, 120, 0.18),

    "Hydro":   (1968, 18, 220, 0.15),

    "Gas":     (1995, 14, 350, 0.20),

    "Nuclear": (1982,  9, 900, 0.05),

    "Solar":   (2016,  4,  60, 0.10),

    "Oil":     (1975, 15, 150, 0.06),

    "Biomass": (2000, 10,  40, 0.04),

}

SITES = ["Falcon", "La Amistad", "Palo Verde", "Wind Ridge", "Coal Basin",

         "Sun Mesa", "Rio Bravo", "High Plains", "Great Lakes", "Blue River"]

OWNERS = ["NextEra", "Duke Energy", "Southern Co", "Iberdrola", "ENGIE",

          "CFE", "RWE", "State Grid", "NTPC", "Brookfield"]





def make_fake_power_plants(n: int = 6000, seed: int = 42) -> pd.DataFrame:

    rng = np.random.default_rng(seed)



    codes = list(COUNTRIES)

    fuels = list(FUELS)

    country = rng.choice(codes, size=n, p=[COUNTRIES[c][3] for c in codes])

    fuel1 = rng.choice(fuels, size=n, p=[FUELS[f][3] for f in fuels])



    comm_year = np.zeros(n)

    capacitymw = np.zeros(n)

    for f in fuels:                                   # fuel-specific era + size

        m = fuel1 == f

        k = int(m.sum())

        comm_year[m] = rng.normal(FUELS[f][0], FUELS[f][1], k)

        capacitymw[m] = rng.lognormal(np.log(FUELS[f][2]), 0.7, k)

    comm_year = np.clip(np.rint(comm_year), 1900, 2020)

    comm_year[rng.random(n) < 0.06] = 0.0             # "unknown" year, like the slide

    capacitymw = np.round(capacitymw, 1)



    latitude, longitude = np.empty(n), np.empty(n)

    for c in codes:

        m = country == c

        k = int(m.sum())

        latitude[m] = COUNTRIES[c][1] + rng.normal(0, 6, k)

        longitude[m] = COUNTRIES[c][2] + rng.normal(0, 8, k)



    known = comm_year > 0

    cap_year = np.where(known & (rng.random(n) < 0.3),

                        comm_year - rng.integers(0, 5, n), 0).astype(int)

    cf = rng.uniform(0.25, 0.60, n)                   # capacity factor

    gwh_2015 = np.round(capacitymw * cf * 8.76, 1)

    gwh_2016 = np.round(gwh_2015 * rng.uniform(0.9, 1.1, n), 1)

    gwh_estimt = np.round(gwh_2015 * rng.uniform(0.9, 1.1, n), 3)



    geo = [{"type": "Point", "coordinates": [round(lo, 6), round(la, 6)]}

           for lo, la in zip(longitude, latitude)]

    name = np.array([f"{rng.choice(SITES)} {f} #{i:04d}" for i, f in enumerate(fuel1)])



    return pd.DataFrame({

        "geo": geo,

        "cap_year": cap_year,

        "capacitymw": capacitymw,

        "comm_year": comm_year.astype(float),

        "country": country,

        "country_lg": [COUNTRIES[c][0] for c in country],

        "fuel1": fuel1,

        "fuel2": np.where(rng.random(n) < 0.12, rng.choice(["Gas", "Oil"], n), ""),

        "fuel3": np.where(rng.random(n) < 0.03, rng.choice(["Gas", "Oil"], n), ""),

        "fuel4": np.full(n, ""),

        "gwh_2015": gwh_2015,

        "gwh_2016": gwh_2016,

        "gwh_estimt": gwh_estimt,

        "latitude": np.round(latitude, 5),

        "longitude": np.round(longitude, 5),

        "name": name,

        "owner": rng.choice(OWNERS, n),

    })





# ----------------------------------------------------------------------------------

# Stand-ins for the slide's `dask_ee` reader and Dask's `.compute()`

# ----------------------------------------------------------------------------------

def read_ee(fc: str):

    """Mimic `dask_ee.read_ee(fc)`: load the (fake) energy dataset, lazily if Dask exists."""

    backend = "dask" if dd is not None else "pandas"

    print(f"[read_ee] loading {fc!r} with the {backend} backend")

    pdf = make_fake_power_plants()

    return dd.from_pandas(pdf, npartitions=4) if dd is not None else pdf





def _compute(df, scheduler="threads"):

    """The slide's `.compute(scheduler="threads")`; identity operation for pandas."""

    if dd is not None and isinstance(df, dd.DataFrame):

        return df.compute(scheduler=scheduler)

    return df





# ----------------------------------------------------------------------------------

# The pipeline exactly as chained on the slide

# ----------------------------------------------------------------------------------

def run_pipeline(df):

    return (

        df[df.comm_year.gt(1940) & df.country.eq("USA") & df.fuel1.isin(["Coal", "Wind"])]

        .astype({"comm_year": int})

        .drop(columns=["geo"])

        .groupby(["comm_year", "fuel1"])

        .agg({"capacitymw": "sum"})

        .reset_index()

        .sort_values(by=["comm_year"])

        .pipe(_compute, scheduler="threads")   # slide: .compute(scheduler="threads")

        .pivot_table(index="comm_year", columns="fuel1", values="capacitymw", fill_value=0)

        .plot()

    )





def main() -> None:

    fc = "fake://global-power-plants.parquet"   # stand-in for the real feature collection

    df = read_ee(fc)



    print("\nRaw dataset preview (columns as on the slide):")

    print(df.head(2).to_string(max_colwidth=18), "\n")



    ax = run_pipeline(df)

    print(ax)                                   # e.g. <Axes: xlabel='comm_year'>



    ax.set_title("USA commissioned capacity by year and fuel")

    ax.set_ylabel("capacity (MW)")

    fig = ax.get_figure()

    fig.set_size_inches(10, 5)

    out = "usa_capacity_coal_wind.png"

    fig.savefig(out, dpi=150, bbox_inches="tight")

    print(f"Plot saved to {out}")



    if matplotlib.get_backend().lower() != "agg":

        plt.show()





if __name__ == "__main__":

    main()
