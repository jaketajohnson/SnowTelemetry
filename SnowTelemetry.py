"""
 SYNOPSIS

     SnowLines.py

 DESCRIPTION

     This script performs RouteStats and SnowLines processing

 REQUIREMENTS

     Python 3
     arcpy
 """

import arcpy
import logging
import os
import sys
import traceback
from logging.handlers import RotatingFileHandler


def start_rotating_logging(log_path=None,
                           max_bytes=500000,
                           backup_count=1,
                           suppress_requests_messages=True):
    """
    This function starts logging with a rotating file handler.  If no log
    path is provided it will start logging in the same folder as the script,
    with the same name as the script.

    Parameters
    ----------
    log_path : str
        the path to use in creating the log file
    max_bytes : int
        the maximum number of bytes to use in each log file
    backup_count : int
        the number of backup files to create
    suppress_requests_messages : bool
        If True, then SSL warnings from the requests and urllib3
        modules will be suppressed

    Returns
    -------
    the_logger : logging.logger
        the logger object, ready to use
    """
    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    # If no log path was provided, construct one
    script_path = sys.argv[0]
    script_folder = os.path.dirname(script_path)
    script_name = os.path.splitext(os.path.basename(script_path))[0]
    if not log_path:
        log_path = os.path.join(script_folder, "{}.log".format(script_name))

    # Start logging
    the_logger = logging.getLogger(script_name)
    the_logger.setLevel(logging.DEBUG)

    # Add the rotating file handler
    log_handler = RotatingFileHandler(filename=log_path,
                                      maxBytes=max_bytes,
                                      backupCount=backup_count)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(formatter)
    the_logger.addHandler(log_handler)

    # Add the console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    the_logger.addHandler(console_handler)

    # Suppress SSL warnings in logs if instructed to
    if suppress_requests_messages:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    return the_logger


def is_valid_path(parser, path):
    """
    Check to see if a provided path is valid.  Works with argparse

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The argument parser object
    path : str
        The path to evaluate whether it exists or not

    Returns
    ----------
    path : str
        If the path exists, it is returned  if not, a
        parser.error is raised.
    """
    if not os.path.exists(path):
        parser.error("The path {0} does not exist!".format(path))
    else:
        return path


def SnowLines():
    # Environment
    arcpy.env.overwriteOutput = True
    spatial_reference = arcpy.SpatialReference(3436)

    # SDE Paths
    fgdb_folder = r"F:\Shares\FGDB_Services"
    # county_parcels_sde = os.path.join(fgdb_folder, r"DatabaseConnections\COSPW@imSPFLD@MCWINTCWDB.sde\imSPFLD.COSPW.Zoning\imSPFLD.COSPW.CountyParcels")
    db_view = os.path.join(fgdb_folder, r"DatabaseConnections\COSPW@AVL@MCWINTCWDB.sde")
    avl_table = os.path.join(db_view, "AVL.dbo.vw_AVLplow24")
    arcpy.MakeFeatureLayer_management(avl_table, "avl_table", "TEMPORAL < 25 and spd <= 35")

    # Snow Tracks GDB Paths
    telemetry_gdb = os.path.join(fgdb_folder, r"Data\Telemetry.gdb")
    snow_dataset = os.path.join(telemetry_gdb, "Snow")
    # county_parcels = os.path.join(snow_dataset, "Parcels")
    # avl_tracks = os.path.join(telemetry_gdb, "avlTracks")
    avl_24 = os.path.join(telemetry_gdb, "avl_24")
    avl_plow_points = os.path.join(snow_dataset, "avlPlowPoints")
    avl_plow_lines = os.path.join(snow_dataset, "avlPlowLines")
    # snow_parcels = os.path.join(snow_dataset, "SnowParcels")

    # Memory paths
    avl_plow_points_mem = r"in_memory\avl_plow_points_mem"
    avl_plow_lines_mem = r"in_memory\avl_plow_lines_mem"

    # Copy over the avl_24 table to the Telemetry geodatabase
    arcpy.DeleteRows_management(avl_24)
    arcpy.Append_management(avl_table, avl_24, "NO_TEST")

    # Convert the table into a layer then create lines from the points
    arcpy.MakeXYEventLayer_management(avl_24, "lon", "lat", "avlPlowPoints", spatial_reference)
    arcpy.DeleteRows_management(avl_plow_points)
    arcpy.Append_management("avlPlowPoints", avl_plow_points, "NO_TEST")
    arcpy.MakeFeatureLayer_management(avl_plow_points, avl_plow_points_mem)
    arcpy.PointsToLine_management(avl_plow_points_mem, avl_plow_lines_mem, "unitName", "datetime", "NO_CLOSE")
    arcpy.DeleteRows_management(avl_plow_lines)
    arcpy.Append_management(avl_plow_lines_mem, avl_plow_lines, "NO_TEST")
    # arcpy.Delete_management(avl_plow_points_mem)

    # Create a parcel layer showing intersection with SnowTracks; broken, do not remove
    # arcpy.DeleteRows_management(county_parcels)
    # arcpy.Append_management(county_parcels_sde, county_parcels, "NO_TEST")
    # parcel_selection = arcpy.SelectLayerByLocation_management(county_parcels, "WITHIN_A_DISTANCE_GEODESIC", avl_plow_lines, "50 Feet", "NEW_SELECTION")
    # arcpy.DeleteRows_management(snow_parcels)
    # arcpy.CopyFeatures_management(parcel_selection, snow_parcels)
    # arcpy.Append_management(parcel_selection, snow_parcels, "NO_TEST")


def RouteStats():

    # Environment
    arcpy.env.overwriteOutput = True
    arcpy.parallelProcessingFactor = "7"
    arcpy.SetLogHistory(False)

    # Paths
    gdb_folder = r"F:\Shares\FGDB_Services"
    imspfld_sde = os.path.join(gdb_folder, r"DatabaseConnections\COSPW@imSPFLD@MCWINTCWDB.sde")
    sde_routes = os.path.join(imspfld_sde, r"imSPFLD.COSPW.FacilitiesStreets\imSPFLD.COSPW.RoadwayInformation")

    # GDB Paths

    temp_fgdb = os.path.join(gdb_folder, r"Data", r"Telemetry_temp.gdb")
    gdb_routes = os.path.join(gdb_folder, r"Data", r"Telemetry.gdb")
    dissolved_routes_temp = os.path.join(temp_fgdb, "Dissolved")
    dissolved_routes_dest = os.path.join(gdb_routes, r"Dissolved")

    # View paths
    avl_sde = os.path.join(gdb_folder, r"DatabaseConnections\COSPW@AVL@MCWINTCWDB.sde")
    avl_table = os.path.join(avl_sde, r"AVL.dbo.vw_AVLplow24")

    if not arcpy.Exists(temp_fgdb):
        arcpy.CreateFileGDB_management(gdb_folder, r"Data\Telemetry_temp.gdb")

    if not arcpy.Exists(gdb_routes):
        arcpy.CreateFileGDB_management(gdb_folder, r"Data\Telemetry.gdb")

    # Create new routes then save to the GDB
    arcpy.MakeFeatureLayer_management(sde_routes, "gdb_routes",
                                      "SNOW_FID <> 'NORTE'")
    arcpy.Dissolve_management("gdb_routes", dissolved_routes_temp, ["SNOW_DIST", "SNOW_TYPE", "ROAD_NAME"],
                              "LN_TOTALMI SUM")
    arcpy.DeleteRows_management(dissolved_routes_dest)
    arcpy.Append_management(dissolved_routes_temp, dissolved_routes_dest, "NO_TEST")

    # Selection queries for each type of snow district/route
    arcpy.MakeFeatureLayer_management(dissolved_routes_temp, "dissolved_routes1")
    arcpy.MakeFeatureLayer_management(dissolved_routes_temp, "dissolved_routes2")
    arcpy.MakeFeatureLayer_management(dissolved_routes_temp, "dissolved_routes3")
    arcpy.MakeFeatureLayer_management(dissolved_routes_temp, "dissolved_routes4")
    snow_type_1 = arcpy.SelectLayerByAttribute_management("dissolved_routes1", "NEW_SELECTION", "SNOW_TYPE = '1' And SUM_LN_TOTALMI IS NOT NULL")  # Trouble spots
    snow_type_2 = arcpy.SelectLayerByAttribute_management("dissolved_routes2", "NEW_SELECTION", "SNOW_TYPE = '2' And SUM_LN_TOTALMI IS NOT NULL")  # Routes
    snow_type_3 = arcpy.SelectLayerByAttribute_management("dissolved_routes3", "NEW_SELECTION", "SNOW_TYPE = '3' And SUM_LN_TOTALMI IS NOT NULL")  # Sections-Mains
    snow_type_4 = arcpy.SelectLayerByAttribute_management("dissolved_routes4", "NEW_SELECTION", "SNOW_TYPE = '4' And SUM_LN_TOTALMI IS NOT NULL")  # SectionsNeighborhoods

    # Summarized layer temporary outputs
    avl_plow_traffic_all_temp = os.path.join(temp_fgdb, "avlPlowTrafficAll")
    avl_plow_traffic_1_temp = os.path.join(temp_fgdb, "avlPlowTraffic1")
    avl_plow_traffic_2_temp = os.path.join(temp_fgdb, "avlPlowTraffic2")
    avl_plow_traffic_3_temp = os.path.join(temp_fgdb, "avlPlowTraffic3")
    avl_plow_traffic_4_temp = os.path.join(temp_fgdb, "avlPlowTraffic4")

    # Summarized layer final destination outputs
    avl_plow_traffic_all_dest = os.path.join(gdb_routes, "avlPlowTrafficAll")
    avl_plow_traffic_1_dest = os.path.join(gdb_routes, "avlPlowTraffic1")
    avl_plow_traffic_2_dest = os.path.join(gdb_routes, "avlPlowTraffic2")
    avl_plow_traffic_3_dest = os.path.join(gdb_routes, "avlPlowTraffic3")
    avl_plow_traffic_4_dest = os.path.join(gdb_routes, "avlPlowTraffic4")

    # A list of lists: [0]=selection input, [1]=temp output, [2]=buffer radius, [3]=final destination (not the movies)
    snow_types = [[snow_type_1, avl_plow_traffic_1_temp, 50, avl_plow_traffic_1_dest],
                  [snow_type_2, avl_plow_traffic_2_temp, 50, avl_plow_traffic_2_dest],
                  [snow_type_3, avl_plow_traffic_3_temp, 25, avl_plow_traffic_3_dest],
                  [snow_type_4, avl_plow_traffic_4_temp, 25, avl_plow_traffic_4_dest]]

    # Accessing the list of lists to create four separate layers
    for snow_type in snow_types:

        # Summarize
        arcpy.MakeFeatureLayer_management(avl_table, "avl_table", "TEMPORAL < 25 and spd <= 35")
        arcpy.SummarizeNearby_analysis(snow_type[0], "avl_table", snow_type[1], "STRAIGHT_LINE", snow_type[2], "FEET")

        # Add/calculate fields
        arcpy.AddFields_management(snow_type[1],
                                   [["dotsperlanemile", "double", "Dots Per Lane Mile"],
                                    ["dotsperlanemilemax", "double", "Max Dots per Lane Mile"],
                                    ["percentage", "double", "Percentage"],
                                    ["log_percentage", "double", "log1p(percentage)"]])
        arcpy.CalculateField_management(snow_type[1], "dotsperlanemile", "!Point_Count!/!SUM_LN_TOTALMI!", "PYTHON3")

        # Grab the MAX dotspermile value and fill out dotspermilemax in the summary feature class
        maximum = 0.0
        with arcpy.da.SearchCursor(snow_type[1], "dotsperlanemile") as sc:
            for a_row in sc:
                if a_row[0] > maximum:
                    maximum = a_row[0]

        arcpy.CalculateFields_management(snow_type[1], "PYTHON3", [["dotsperlanemilemax", "{}".format(maximum)],
                                                                   ["percentage", "(!dotsperlanemile!/!dotsperlanemilemax!)*100"],
                                                                   ["log_percentage", "math.log1p(!percentage!)"]])

        # Copy the temp sum tables into the destinations
        arcpy.DeleteRows_management(snow_type[3])
        arcpy.Append_management(snow_type[1], snow_type[3], "NO_TEST")

    # Merge the sum tables into all routes, load into destination
    arcpy.Merge_management([avl_plow_traffic_1_dest, avl_plow_traffic_2_dest, avl_plow_traffic_3_dest, avl_plow_traffic_4_dest], avl_plow_traffic_all_temp)
    arcpy.DeleteRows_management(avl_plow_traffic_all_dest)
    arcpy.Append_management(avl_plow_traffic_all_temp, avl_plow_traffic_all_dest, "NO_TEST")


def main():
    """
    Main execution code
    """
    # Make a few variables to use
    # script_folder = os.path.dirname(sys.argv[0])
    log_file_folder = r"C:\Scripts\SnowTelemetry\Log_Files"
    script_name_no_ext = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_file = os.path.join(log_file_folder, "{}.log".format(script_name_no_ext))
    logger = None

    try:

        # Get logging going
        logger = start_rotating_logging(log_path=log_file,
                                        max_bytes=100000000,
                                        backup_count=2,
                                        suppress_requests_messages=True)
        logger.info("")
        logger.info("--- Script Execution Started ---")

        RouteStats()
        logger.info("Completed RouteStats processing")

        SnowLines()
        logger.info("Completed SnowLines processing")

    except ValueError as e:
        exc_traceback = sys.exc_info()[2]
        error_text = 'Line: {0} --- {1}'.format(exc_traceback.tb_lineno, e)
        try:
            logger.error(error_text)
        except NameError:
            print(error_text)

    except (IOError, KeyError, NameError, IndexError, TypeError, UnboundLocalError, arcpy.ExecuteError):
        tbinfo = traceback.format_exc()
        try:
            logger.error(tbinfo)
        except NameError:
            print(tbinfo)

    finally:
        # Shut down logging
        try:
            logger.info("--- Script Execution Completed ---")
            logging.shutdown()
        except NameError:
            pass


if __name__ == '__main__':
    main()
