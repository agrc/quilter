# 🧵 quilter 

Have you ever tried to download a bunch of data from raster.utah.gov, clicking one link at a time, and thought "There's got to be an easier way to do this?"

Enter quilter, a python script built on the GDAL python library that will automatically download, extract, merge, and reproject all the datasets in your search using a CSV created by raster.utah.gov (the CSV generator is under development).

quilter works on all the different products available on raster.utah.gov:
* DEMs
* Aerial imagery (greyscale, 3-band RGB, and 4-band RGBN)
* Contours
* Topos, both standard and trimmed
* Historical topos

quilter may also work with the CSVs created by USGS' [National Map Download](https://viewer.nationalmap.gov/basic/), but has not been tested against all their products.

## Installation

quilter depends on the GDAL python bindings and the `requests` library. The supported way to to install these is to use the conda environment manager.

To install quilter:

1. Create a conda environment with the needed packages installed:
   * Clone the ArcGIS Pro default conda environment — GDAL and `requests` are already installed.
   _— or —_
   * Create a new conda environment using environment.yml: `conda env create -f environment.yml`. This installs GDAL via the conda-forge channel.
1. Clone the quilter repository.

>**Why conda-forge?**
>
>The default conda channel's GDAL does not have support for TIFFs larger than 4GB. quilter sets the BigTIFF flag for any TIFFs that it creates (equivalent to the `-co bigtiff=yes` CLI flag).
>
>quilter will still run if you install GDAL from the default channel, but it will give warnings about BigTIFF support and will fail when writing any TIFFs >4GB.
>
>The GDAL package included in ESRI's ArcGIS Pro environment is compiled with BigTIFF support.

# Usage

`python quilter.py csv output_folder` _`-m name`_ _`-p epsg_code`_

By default, quilter will download the files listed in `csv` (generated from raster.utah.gov) to the specified `output_folder`.

* The optional `-m` flag will merge the downloaded files into a single dataset with the given `name` (omit the file extension). 

  >**Note:** We don't recommend merging the normal or historical topos because the collar around the maps overlap. The "collarless" topos merge fine.

* The optional `-p` flag will reproject each downloaded file into the CRS specified by an `epsg_code`. This should be in the format `EPSG:xxxx` or `ESRI:xxxx`. 

  >**Note:** Some .asc files lacking detailed projection info will project to weird locations.

* The `-p` and `-m` flags can be used together to create a single file reprojected to the desired CRS.

## Usage Examples

`python quilter.py dems.csv c:\data\dems -m tville -p EPSG:3566`
  * Download all the DEMs listed in `dems.csv` to `c:\data\dems`
  * Merge the DEMs into the single file `c:\data\tville.tif`
  * Reproject `tville.tif` to the Utah State Plane Central CRS.

`python quilter.py dems.csv c:\data\dems -p EPSG:3566`
  * Download all the DEMs listed in `dems.csv` to `c:\data\dems`
  * Create copies of each DEM reprojected to Utah State Plane Central in `c:\data\reprojected`.

## Development

quilter is developed in VS Code using `pylint` for linting and `yapf` for some formatting. 

The .vscode directory has been included in the repo for those interested in development. Because quilter uses conda, settings.json points to a specific conda environment. This may trigger VS Code to prompt you for the proper environment the first time you open the file.
