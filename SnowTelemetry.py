"""
 SYNOPSIS

     SnowLines.py

 DESCRIPTION

     This script performs RouteStats and SnowLines processing

     SnowLines
     1. Import AVL data as points
     2. Add coordinate information
     3. Create lines with direction out of the AVL points

     RouteStats
     1. Import RoadwayInformation
     2. Dissolve based off of snow route type
     3. Add statistical fields
     4. Calculate each field while looping through route types, each loop creating a new layer
     5. Merge all route types into one large layer

     Precipitation Forecast
     1. Grab the precipitation accumulation forecast service
     2. Prepare service layer with dissolve and add a field
     3. Classify precipitation amounts
     4. Calculate score

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


def start_rotating_logging(log_file=None, max_bytes=100000, backup_count=1, suppress_requests_messages=True):
    """Creates a logger that outputs to stdout and a log file; outputs start and completion of functions or attribution of functions"""

    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Paths to desired log file
    script_folder = os.path.dirname(sys.argv[0])
    script_name = os.path.basename(sys.argv[0])
    script_name_no_ext = os.path.splitext(script_name)[0]
    log_folder = os.path.join(script_folder, "Log_Files")
    if not log_file:
        log_file = os.path.join(log_folder, f"{script_name_no_ext}.log")

    # Start logging
    the_logger = logging.getLogger(script_name)
    the_logger.setLevel(logging.DEBUG)

    # Add the rotating file handler
    log_handler = RotatingFileHandler(filename=log_file, maxBytes=max_bytes, backupCount=backup_count)
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


def Telemetry():

    # Environment
    arcpy.env.overwriteOutput = True
    spatial_reference = arcpy.SpatialReference(3436)

    # Folders
    fgdb_folder = r"F:\Shares\FGDB_Services"
    database_connections = os.path.join(fgdb_folder, "DatabaseConnections")

    # Connection files
    db_view = os.path.join(database_connections, r"COSPW@AVL@MCWINTCWDB.sde")
    sde = os.path.join(database_connections, r"COSPW@imSPFLD@MCWINTCWDB.sde")
    roadway_information = os.path.join(sde, r"imSPFLD.COSPW.FacilitiesStreets\imSPFLD.COSPW.RoadwayInformation")

    # Datasets
    data = os.path.join(fgdb_folder, "Data")
    telemetry = os.path.join(data, "Telemetry.gdb")
    snow_dataset = os.path.join(telemetry, "Snow")

    # Snow tracks
    avl_24 = os.path.join(telemetry, "avl_24")
    avl_plow_lines = os.path.join(snow_dataset, "avlPlowLines")
    dissolved_routes = os.path.join(snow_dataset, "DissolvedRoutes")
    dissolved_routes_temp = os.path.join(snow_dataset, "DissolvedRoutesTemp")
    avl_plow_traffic_all_dest = os.path.join(snow_dataset, "avlPlowTrafficAll")
    avl_plow_traffic_1_dest = os.path.join(snow_dataset, "avlPlowTraffic1")  # Trouble spots
    avl_plow_traffic_2_dest = os.path.join(snow_dataset, "avlPlowTraffic2")  # Routes
    avl_plow_traffic_3_dest = os.path.join(snow_dataset, "avlPlowTraffic3")  # Sections - Mains
    avl_plow_traffic_4_dest = os.path.join(snow_dataset, "avlPlowTraffic4")  # Sections - Neighborhoods

    # Move raw plow data into a working GDB
    avl_table = os.path.join(db_view, r"AVL.dbo.vw_AVLplow24")
    arcpy.MakeFeatureLayer_management(avl_table, "avlTable", "TEMPORAL < 25 and spd <= 35")
    arcpy.FeatureClassToFeatureClass_conversion("avlTable", telemetry, "avl_24")

    def SnowLines():
        """Convert AVL points to lines"""
        arcpy.MakeXYEventLayer_management(avl_24, "lon", "lat", "avlPlowPoints", spatial_reference)
        arcpy.FeatureClassToFeatureClass_conversion("avlPlowPoints", snow_dataset, "avlPlowPoints")
        arcpy.PointsToLine_management("avlPlowPoints", avl_plow_lines, "unitName", "datetime", "NO_CLOSE")

    def RouteStats():
        """Add statistics fields and statistics to the new lines"""

        # Create new routes then save to the GDB
        arcpy.MakeFeatureLayer_management(roadway_information, "RoadwayInformation", "SNOW_FID <> 'NORTE'")
        arcpy.Dissolve_management("RoadwayInformation", dissolved_routes, ["SNOW_DIST", "SNOW_TYPE", "ROAD_NAME"], "LN_TOTALMI SUM", unsplit_lines="UNSPLIT_LINES")
        arcpy.MakeFeatureLayer_management(dissolved_routes, "DissolvedRoutes")

        # Add/calculate fields
        arcpy.AddFields_management("DissolvedRoutes",
                                   [["dotsperlanemile", "double", "Dots Per Lane Mile"],
                                    ["dotsperlanemilemax", "double", "Max Dots per Lane Mile"],
                                    ["percentage", "double", "Percentage"],
                                    ["log_percentage", "double", "log1p(percentage)"]])
        arcpy.MakeFeatureLayer_management(avl_table, "avl_table", "TEMPORAL < 25 and spd <= 35")

        # Summarized layer iteration list
        # [0]=selection definition query, [1]=buffer radius, [2]=final destination
        snow_types = [["SNOW_TYPE = '1' And SUM_LN_TOTALMI IS NOT NULL", 50, "avlPlowTraffic1"],
                      ["SNOW_TYPE = '2' And SUM_LN_TOTALMI IS NOT NULL", 50, "avlPlowTraffic2"],
                      ["SNOW_TYPE = '3' And SUM_LN_TOTALMI IS NOT NULL", 25, "avlPlowTraffic3"],
                      ["SNOW_TYPE = '4' And SUM_LN_TOTALMI IS NOT NULL", 25, "avlPlowTraffic4"]]

        # Create four layers using the above list
        for snow_type in snow_types:

            # Summarize
            route_selection = arcpy.SelectLayerByAttribute_management("DissolvedRoutes", "NEW_SELECTION", snow_type[0])
            arcpy.SummarizeNearby_analysis(route_selection, "avl_table", dissolved_routes_temp, "STRAIGHT_LINE", snow_type[1], "FEET")
            arcpy.MakeFeatureLayer_management(dissolved_routes_temp, "DissolvedRoutesTemp")
            arcpy.CalculateField_management("DissolvedRoutesTemp", "dotsperlanemile", "!Point_Count!/!SUM_LN_TOTALMI!", "PYTHON3")

            # Find maximum dotsperlanemile value per route type
            maximum = 0.0
            clause = (None, "ORDER BY dotsperlanemile DESC")
            with arcpy.da.SearchCursor("DissolvedRoutesTemp", "dotsperlanemile", sql_clause=clause) as cursor:
                for row in cursor:
                    maximum = row[0]
                    if maximum != 0.0:
                        break
            del cursor

            arcpy.CalculateFields_management("DissolvedRoutesTemp", "PYTHON3",
                                             [["dotsperlanemilemax", f"{maximum}"],
                                              ["percentage", "(!dotsperlanemile!/!dotsperlanemilemax!)*100"],
                                              ["log_percentage", "math.log1p(!percentage!)"]])
            arcpy.FeatureClassToFeatureClass_conversion("DissolvedRoutesTemp", snow_dataset, f"{snow_type[2]}")
            logger.info(f"--- --- {snow_type[2]} Complete")

        # Merge the sum tables into one new layer
        arcpy.Merge_management([avl_plow_traffic_1_dest, avl_plow_traffic_2_dest, avl_plow_traffic_3_dest, avl_plow_traffic_4_dest], avl_plow_traffic_all_dest)
        arcpy.Delete_management(dissolved_routes_temp)

    def PrecipitationForecast():
        """Calculate the severity of a snow event on each snow route"""

        # National Weather Service Precipitation Forecast, Cumulative Total
        service = "https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services/NDFD_Precipitation_v1/FeatureServer/2"
        # TODO: Switch to snow map when finished

        # Paths
        cumulative = os.path.join(snow_dataset, "CumulativeTotal")

        # Preparing layers
        arcpy.AddFields_management(avl_plow_traffic_all_dest, [["Forecast", "TEXT", "Forecast", 20, "0"],
                                                               ["Severity", "SHORT", "Severity", 1, 0]])
        arcpy.Dissolve_management(service, cumulative, "label")
        arcpy.MakeFeatureLayer_management(avl_plow_traffic_all_dest, "avl_all")
        arcpy.MakeFeatureLayer_management(cumulative, "Cumulative")
        arcpy.CalculateField_management("avl_all", "Forecast", "'0 to 0 inches'")

        # Expressions
        severities = [["Forecast IS NULL or Forecast = '0 to 0 inches'", "0"],
                      ["Forecast IN ('0.01 to 0.10 inches', '0.10 to 0.25 inches', '0.50 to 0.75 inches', '0.25 to 0.50 inches', '0.75 to 1.00 inches')", "1"],
                      ["Forecast IN ('1.00 to 1.50 inches', '1.50 to 2.00 inches')", "2"],
                      ["Forecast IN ('2.00 to 2.50 inches', '2.50 to 3.00 inches')", "3"],
                      ["Forecast = '3.00 to 4.00 inches'", "4"],
                      ["Forecast = '4.00 to 5.00 inches'", "5"],
                      ["Forecast = '5.00 to 6.00 inches'", "6"],
                      ["Forecast = '6.00 to 7.00 inches'", "7"],
                      ["Forecast = '7.00 to 8.00 inches'", "8"]]

        # Analysis
        with arcpy.da.UpdateCursor("Cumulative", "label") as cursor:
            for row in cursor:
                selected_cumulative = arcpy.SelectLayerByAttribute_management("Cumulative", "NEW_SELECTION", f"label = '{row[0]}'")
                selected_avl = arcpy.SelectLayerByLocation_management("avl_all", "HAVE_THEIR_CENTER_IN", selected_cumulative)
                arcpy.CalculateField_management(selected_avl, "Forecast", f"'{row[0]}'", "PYTHON3")

        for severity in severities:
            selected_avl = arcpy.SelectLayerByAttribute_management("avl_all", "NEW_SELECTION", severity[0])
            arcpy.CalculateField_management(selected_avl, "Severity", severity[1], "PYTHON3")

    # Run the above functions with logger error catching and formatting

    logger = start_rotating_logging()

    try:

        logger.info("")
        logger.info("--- Script Execution Started ---")

        logger.info("--- --- --- --- Snow Lines Start")
        SnowLines()
        logger.info("--- --- --- --- Snow Lines Complete")

        logger.info("--- --- --- --- Route Stats Start")
        RouteStats()
        logger.info("--- --- --- --- Route Stats Complete")

        logger.info("--- --- --- --- Precipitation Forecast Start")
        PrecipitationForecast()
        logger.info("--- --- --- --- Precipitation Forecast Complete")

    except (IOError, KeyError, NameError, IndexError, TypeError, UnboundLocalError):
        tbinfo = traceback.format_exc()
        try:
            logger.error(tbinfo)
        except NameError:
            print(tbinfo)

    except arcpy.ExecuteError:
        try:
            tbinfo = traceback.format_exc(2)
            logger.error(tbinfo)
        except NameError:
            print(arcpy.GetMessages(2))

    except:
        logger.exception("Picked up an exception:")

    finally:
        try:
            logger.info("--- Script Execution Completed ---")
            logging.shutdown()
        except NameError:
            pass


def main():
    Telemetry()


if __name__ == '__main__':
    main()
