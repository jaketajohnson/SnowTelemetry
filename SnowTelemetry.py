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

 REQUIREMENTS

     Python 3
     arcpy
 """

import arcpy
import logging
import os
import sys
import traceback


def ScriptLogging():
    """Enables console and log file logging; see test script for comments on functionality"""
    current_directory = os.getcwd()
    script_filename = os.path.basename(sys.argv[0])
    log_filename = os.path.splitext(script_filename)[0]
    log_file = os.path.join(current_directory, f"{log_filename}.log")
    if not os.path.exists(log_file):
        with open(log_file, "w"):
            pass
    message_formatting = "%(asctime)s - %(levelname)s - %(message)s"
    date_formatting = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=message_formatting, datefmt=date_formatting)
    logging_output = logging.getLogger(f"{log_filename}")
    logging_output.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logging_output.addHandler(console_handler)
    logging.basicConfig(format=message_formatting, datefmt=date_formatting, filename=log_file, filemode="w", level=logging.INFO)
    return logging_output


def Telemetry():

    # Logging
    def logging_lines(name):
        """Use this wrapper to insert a message before and after the function for logging purposes"""
        if type(name) == str:
            def logging_decorator(function):
                def logging_wrapper():
                    logger.info(f"{name} Start")
                    function()
                    logger.info(f"{name} Complete")
                return logging_wrapper
            return logging_decorator
    logger = ScriptLogging()
    logger.info("Script Execution Start")

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
    avl_48_simple = os.path.join(telemetry, "avl_48_simple")
    avl_plow_lines = os.path.join(snow_dataset, "avlPlowLines")
    dissolved_routes = os.path.join(snow_dataset, "DissolvedRoutes")
    dissolved_routes_temp = os.path.join(snow_dataset, "DissolvedRoutesTemp")
    dissolved_routes_simple = os.path.join(snow_dataset, "DissolvedRoutesSimple")
    avl_plow_traffic_all_dest = os.path.join(snow_dataset, "avlPlowTrafficAll")
    avl_plow_traffic_1_dest = os.path.join(snow_dataset, "avlPlowTraffic1")  # Trouble spots
    avl_plow_traffic_2_dest = os.path.join(snow_dataset, "avlPlowTraffic2")  # Routes
    avl_plow_traffic_3_dest = os.path.join(snow_dataset, "avlPlowTraffic3")  # Sections - Mains
    avl_plow_traffic_4_dest = os.path.join(snow_dataset, "avlPlowTraffic4")  # Sections - Neighborhoods

    # Move raw plow data into a working GDB
    avl_table_24 = os.path.join(db_view, r"AVL.dbo.vw_AVLpLow24")
    arcpy.MakeFeatureLayer_management(avl_table_24, "avlTable24", "TEMPORAL < 25 and spd <= 35")
    arcpy.FeatureClassToFeatureClass_conversion("avlTable24", telemetry, "avl_24")
    avl_table_48 = os.path.join(db_view, r"AVL.dbo.vw_AVLpLow48")
    arcpy.MakeFeatureLayer_management(avl_table_48, "avlTable48", "spd <= 100")
    arcpy.FeatureClassToFeatureClass_conversion("avlTable48", telemetry, "avl_48")

    @logging_lines("Snow Lines")
    def SnowLines():
        """Convert AVL points to lines"""
        arcpy.MakeXYEventLayer_management(avl_24, "lon", "lat", "avlPlowPoints", spatial_reference)
        arcpy.FeatureClassToFeatureClass_conversion("avlPlowPoints", snow_dataset, "avlPlowPoints")
        arcpy.PointsToLine_management("avlPlowPoints", avl_plow_lines, "unitName", "datetime", "NO_CLOSE")

    @logging_lines("Route Stats")
    def RouteStats():
        """Add statistics fields and statistics to the new lines"""

        # Create new routes then save to the GDB
        arcpy.MakeFeatureLayer_management(roadway_information, "RoadwayInformation", "SNOW_FID <> 'NORTE'")
        arcpy.Dissolve_management("RoadwayInformation", dissolved_routes, ["SNOW_DIST", "SNOW_TYPE", "ROAD_NAME", "SNOW_FID", "SNOW__RT_NBR"], "LN_TOTALMI SUM", unsplit_lines="UNSPLIT_LINES")
        arcpy.MakeFeatureLayer_management(dissolved_routes, "DissolvedRoutes")

        # Add/calculate fields
        arcpy.AddFields_management("DissolvedRoutes",
                                   [["dotsperlanemile", "double", "Dots Per Lane Mile"],
                                    ["dotsperlanemilemax", "double", "Max Dots per Lane Mile"],
                                    ["percentage", "double", "Percentage"],
                                    ["log_percentage", "double", "log1p(percentage)"]])
        arcpy.MakeFeatureLayer_management(avl_table_24, "avl_table", "TEMPORAL < 25 and spd <= 35")

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
            logger.info(f"{snow_type[2]} Complete")

        # Merge the sum tables into one new layer
        arcpy.Merge_management([avl_plow_traffic_1_dest, avl_plow_traffic_2_dest, avl_plow_traffic_3_dest, avl_plow_traffic_4_dest], avl_plow_traffic_all_dest)
        arcpy.Delete_management(dissolved_routes_temp)

    @logging_lines("Simple AVL Points")
    def SimplePoints():
        """Simplify AVL points into one feature for display on a web map"""
        arcpy.Dissolve_management("avlTable48", avl_48_simple, "Temporal")

    @logging_lines("Simple Routes")
    def SimpleRoutes():
        """Simplify snow routes by district and priority for display on a web map"""
        arcpy.MakeFeatureLayer_management(roadway_information, "RoadwayInformation", "SNOW_FID <> 'NORTE'")
        arcpy.Dissolve_management("RoadwayInformation", dissolved_routes_simple, ["SNOW_DIST", "SNOW_TYPE"])

    # Try running above scripts
    try:
        SnowLines()
        RouteStats()
        SimplePoints()
        SimpleRoutes()
    except (IOError, KeyError, NameError, IndexError, TypeError, UnboundLocalError, ValueError):
        traceback_info = traceback.format_exc()
        try:
            logger.info(traceback_info)
        except NameError:
            print(traceback_info)
    except arcpy.ExecuteError:
        try:
            logger.error(arcpy.GetMessages(2))
        except NameError:
            print(arcpy.GetMessages(2))
    except:
        logger.exception("Picked up an exception!")
    finally:
        try:
            logger.info("Script Execution Complete")
        except NameError:
            pass


def main():
    Telemetry()


if __name__ == '__main__':
    main()
