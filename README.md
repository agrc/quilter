# quilter

Have you ever tried to download a bunch of data from raster.utah.gov, clicking one link at a time, and thought "There's got to be an easier way to do this?"

Enter quilter, a python script built on the GDAL python library that will automatically download, extract, merge, and reproject all the datasets in your search.

quilter works on all the different products available on raster.utah.gov:
* DEMs
* Aerial imagery (greyscale, 3-band RGB, and 4-band RGBN)
* Contours
* Topos, both standard and trimmed
* Historical topos

quilter may also work on the CSVs created by USGS' [National Map Download](https://viewer.nationalmap.gov/basic/), but has not been tested against all their products.

## Dependencies

* GDAL python bindings
* `requests` library

## Installation

quilter depends on the GDAL python bindings. The recommended and supported way to to install these bindings is to use the conda environment manager (especially on Windows). There are two options for getting the needed dependencies:

1. Clone an ArcGIS Pro default conda environment — GDAL and `requests` are already installed.
1. Create a new conda environment using environment.yml: `conda env create -f environment.yml`. This installs GDAL via the conda-forge channel.

#### Why conda-forge?

The GDAL package in the default conda channel does not have BigTIFF support for TIFFs larger than 4GB. quilter sets the BigTIFF flag for any TIFFs that it creates (equivalent to the `-co bigtiff=yes` CLI flag).

quilter will still run properly for TIFFs <4GB if you install GDAL from the default channel, but it will give warnings about BigTIFF support.

The GDAL package included in ESRI's ArcGIS Pro environment is compiled with BigTIFF support.

# Usage

`python quilter.py path_to_csv output_folder -m merged_name -p epsg_code`

By default, quilter will download the files listed in a csv file generated from raster.utah.gov to a specified directory.

The `-m` flag will merge the downloaded files into a single dataset with the given name (omit the file extension). quilter assumes all the files are spatially contiguous; merging files with large gaps between them will create huge output file sizes.

>Note: While quilter can merge the standard and historical topos, we don't recommend it because the collar around the maps overlap. The "collarless" topos should merge just fine if you want to create your own custom mosaic.

The `-p` flag will reproject each downloaded file into the specified CRS. 

The `-p` and `-m` flags can be used together to create a single file reprojected to the desired CRS.

## Examples

* `python quilter.py dems.csv c:\data\dems -m tville -p EPSG:3566`
  * Download all the DEMs listed in `dems.csv` to `c:\data\dems`
  * Merge the DEMs into the single file `c:\data\tville.tif`
  * Reproject `tville.tif` to the Utah State Plane Central CRS.

* `python quilter.py dems.csv c:\data\dems -p EPSG:3566`
  * Download all the DEMS listed in `dems.csv` to `c:\data\dems`
  * Create copies of each DEM reprojected to Utah State Plane Central in `c:\data\reprojected`.
