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
import os
import sys
import traceback
sys.path.insert(0, "C:/Scripts")
import Logging

# Environment
arcpy.env.overwriteOutput = True
spatial_reference = arcpy.SpatialReference(3436)

# Folders
root = r"F:\Shares\FGDB_Services"
database_connections = os.path.join(root, "DatabaseConnections")

# Connection files
avl_root = os.path.join(database_connections, r"COSPW@AVL@MCWINTCWDB.sde")
sde = os.path.join(database_connections, r"COSPW@imSPFLD@MCWINTCWDB.sde")
roadway_information = os.path.join(sde, r"imSPFLD.COSPW.FacilitiesStreets\imSPFLD.COSPW.RoadwayInformation")

# Datasets
data = os.path.join(root, "Data")
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

# AVL
avl_table_24 = os.path.join(avl_root, r"AVL.dbo.vw_AVLpLow24")
avl_table_48 = os.path.join(avl_root, r"AVL.dbo.vw_AVLpLow48")


# Move raw plow data into a working GDB
@Logging.insert("Initialize", 1)
def initialize():

    # 24 Hour
    arcpy.MakeFeatureLayer_management(avl_table_24, "avlTable24", "TEMPORAL < 25 and spd <= 35")
    arcpy.FeatureClassToFeatureClass_conversion("avlTable24", telemetry, "avl_24")

    # 48 Hour
    arcpy.MakeFeatureLayer_management(avl_table_48, "avlTable48", "spd <= 100")
    arcpy.FeatureClassToFeatureClass_conversion("avlTable48", telemetry, "avl_48")


@Logging.insert("Snow Lines", 1)
def snow_lines():
    """Convert AVL points to lines"""
    arcpy.MakeXYEventLayer_management(avl_24, "lon", "lat", "avlPlowPoints", spatial_reference)
    arcpy.FeatureClassToFeatureClass_conversion("avlPlowPoints", snow_dataset, "avlPlowPoints")
    arcpy.PointsToLine_management("avlPlowPoints", avl_plow_lines, "unitName", "datetime", "NO_CLOSE")


@Logging.insert("Route Stats", 1)
def route_stats():
    """Add statistics fields and statistics to the new lines"""

    # Create new routes then save to the GDB
    Logging.logger.info(f"------START Dissolve")
    arcpy.MakeFeatureLayer_management(roadway_information, "RoadwayInformation", "SNOW_FID <> 'NORTE'")
    arcpy.Dissolve_management("RoadwayInformation", dissolved_routes, ["SNOW_DIST", "SNOW_TYPE", "ROAD_NAME", "SNOW_FID", "SNOW__RT_NBR"], "LN_TOTALMI SUM", unsplit_lines="UNSPLIT_LINES")
    arcpy.MakeFeatureLayer_management(dissolved_routes, "DissolvedRoutes")
    Logging.logger.info(f"------FINISH Dissolve")

    # Add/calculate fields
    Logging.logger.info(f"------START Add/Calculate Fields")
    arcpy.AddFields_management("DissolvedRoutes",
                               [["dotsperlanemile", "double", "Dots Per Lane Mile"],
                                ["dotsperlanemilemax", "double", "Max Dots per Lane Mile"],
                                ["percentage", "double", "Percentage"],
                                ["log_percentage", "double", "log1p(percentage)"]])
    arcpy.MakeFeatureLayer_management(avl_table_24, "avl_table", "TEMPORAL < 25 and spd <= 35")
    Logging.logger.info(f"------FINISH Add/Calculate Fields")

    # Summarized layer iteration list
    # [0]=selection definition query, [1]=buffer radius, [2]=final destination
    snow_types = [["SNOW_TYPE = '1' And SUM_LN_TOTALMI IS NOT NULL", 50, "avlPlowTraffic1"],
                  ["SNOW_TYPE = '2' And SUM_LN_TOTALMI IS NOT NULL", 50, "avlPlowTraffic2"],
                  ["SNOW_TYPE = '3' And SUM_LN_TOTALMI IS NOT NULL", 25, "avlPlowTraffic3"],
                  ["SNOW_TYPE = '4' And SUM_LN_TOTALMI IS NOT NULL", 25, "avlPlowTraffic4"]]

    # Create four layers using the above list
    Logging.logger.info(f"------START Snow Type Layers")
    for snow_type in snow_types:
        Logging.logger.info(f"---------START {snow_type[2]}")

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
        Logging.logger.info(f"---------FINISH {snow_type[2]}")
    Logging.logger.info(f"------FINISH Snow Type Layers")

    # Merge the sum tables into one new layer
    Logging.logger.info(f"------START Merge")
    arcpy.Merge_management([avl_plow_traffic_1_dest, avl_plow_traffic_2_dest, avl_plow_traffic_3_dest, avl_plow_traffic_4_dest], avl_plow_traffic_all_dest)
    Logging.logger.info(f"------FINISH Merge")


@Logging.insert("Simple Points", 1)
def simple_points():
    """Simplify AVL points into one feature for display on a web map"""
    arcpy.Dissolve_management("avlTable48", avl_48_simple, "Temporal")


@Logging.insert("Simple Routes", 1)
def simple_routes():
    """Simplify snow routes by district and priority for display on a web map"""
    arcpy.MakeFeatureLayer_management(roadway_information, "RoadwayInformation", "SNOW_FID <> 'NORTE'")
    arcpy.Dissolve_management("RoadwayInformation", dissolved_routes_simple, ["SNOW_DIST", "SNOW_TYPE"])


if __name__ == "__main__":
    traceback_info = traceback.format_exc()
    try:
        Logging.logger.info("Script Execution Started")
        initialize()
        snow_lines()
        route_stats()
        simple_points()
        simple_routes()
        Logging.logger.info("Script Execution Finished")
    except (IOError, NameError, KeyError, IndexError, TypeError, UnboundLocalError, ValueError):
        Logging.logger.info(traceback_info)
    except NameError:
        print(traceback_info)
    except arcpy.ExecuteError:
        Logging.logger.error(arcpy.GetMessages(2))
    except:
        Logging.logger.info("An unspecified exception occurred")
