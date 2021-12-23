##############################################################################
# ImageFactory - This class follows the concepts and high-level image
# generation approach outlined in "A Deep Learning Streaming Methodology fo
# Trajectory Classification" academic paper upon which the project
# implementation is based.
#
# Author: Joseph Krozak     UNI: JKK2139    Date 12/08/2021
##############################################################################

import numpy as np
from skimage.draw import line
from skimage.draw import line_aa


def bounded_value(n, min_n, max_n):
    if n < min_n:
        return min_n
    elif n > max_n:
        return max_n
    else:
        return n


class ImageFactory:
    N = 224  # Image will be created in N x N normalized grid

    posit_color = [0, 0, 0]  # Black

    speed_color = \
        {
            0:  [255, 0, 0], 1: [255, 0, 0],  # Red
            2:  [255, 0, 0], 3: [255, 0, 0],  # Red
            4:  [0, 0, 255], 5: [0, 0, 255],  # Blue
            6:  [0, 0, 255], 7: [0, 0, 255],  # Blue
            8:  [0, 238, 238], 9: [0, 238, 238],  # Cyan
            10: [0, 238, 238], 11: [0, 238, 238],  # Cyan
            12: [255, 255, 0], 13: [255, 255, 0],  # Yellow
            14: [255, 255, 0], 15: [255, 255, 0],  # Yellow
            16: [0, 255, 0], 17: [0, 255, 0],  # Green
            18: [0, 255, 0], 19: [0, 255, 0],  # Green
            20: [0, 255, 0], 21: [0, 255, 0],  # Green
            22: [0, 255, 0]  # Green
        }

    def __init__(self, in_output_image_path):
        self.output_image_path = in_output_image_path

    def generateImage(self, in_posit_data):
        normalized_posits = self.normalizePosits(in_posit_data)
        posit_track_image = self.createImage(normalized_posits)

        return posit_track_image

    def createImage(self, in_norm_posit_data):
        # Create an N x N image array, initializing all pixels white (255).
        img = np.full((ImageFactory.N, ImageFactory.N, 3), 0xFFFFFF, dtype=np.uint8)
        norm_posit_data_values = list(in_norm_posit_data.values())

        # Plot all normalized posit points on the image

        for norm_posit in norm_posit_data_values:
            img[norm_posit[1], norm_posit[0]] = ImageFactory.posit_color

        # Draw colored (i.e. reflecting speed) line segments connecting the
        # posit points, indicating vessel track pattern

        offset_norm_posit_data_values = norm_posit_data_values[1:]
        paired_norm_posit_values = zip(norm_posit_data_values, offset_norm_posit_data_values)

        for from_posit, to_posit in paired_norm_posit_values:
            rr, cc, val = line_aa(from_posit[1],from_posit[0],to_posit[1],to_posit[0])
            img[rr, cc] = from_posit[2]

        return img

    def normalizePosits(self, in_posit_data):
        # Calculate the total horizontal and vertical distances traveled
        # across the supplied posits data (i.e. vessel track segment).

        posit_data_values = list(in_posit_data.values())
        latitudes = [item[0] for item in posit_data_values]
        longitudes = [item[1] for item in posit_data_values]

        max_longitude = max(longitudes, key=float)
        min_longitude = min(longitudes, key=float)
        distance_x = max_longitude - min_longitude
        #print('Max Long: ' + str(max_longitude) + ', Min Long:  ' + str(min_longitude))
        max_latitude = max(latitudes, key=float)
        min_latitude = min(latitudes, key=float)
        #print('Max Lat: ' + str(max_latitude) + ', Min Lat:  ' + str(min_latitude))
        distance_y = max_latitude - min_latitude
        #print('Dist X: ' + str(distance_x) + ', Dist Y:  ' + str(distance_y))
        # Initialize output list of normalized posit positional /
        # speed color tuples keyed by date/time

        normalized_posits_by_date_time = {}

        # For each supplied posit...

        for posit_date_time in in_posit_data:
            posit_value = in_posit_data[posit_date_time]

            # Calculate posit relative distances from minimums
            posit_distance_x = posit_value[1] - min_longitude
            posit_distance_y = posit_value[0] - min_latitude

            # Calculate normalized posit relative distance from mins.
            norm_posit_dist_x = norm_posit_dist_y = 0

            if distance_x > 0 and distance_y > 0:
                norm_posit_dist_x = posit_distance_x / distance_x
                norm_posit_dist_y = posit_distance_y / distance_y

            # Calculate posit pixel position on N x N grid

            posit_pixel_x = bounded_value( \
                round(norm_posit_dist_x * ImageFactory.N), 0, ImageFactory.N - 1)

            posit_pixel_y = bounded_value( \
                round(norm_posit_dist_y * ImageFactory.N), 0, ImageFactory.N - 1)

            # Calculate posit pixel color based on speed over ground

            posit_pixel_color = ImageFactory.speed_color[bounded_value(round(posit_value[2]), 0, 22)]

            # Finally, accumulate normalized, colored posit tuples
            # by date / time for return

            normalized_posits_by_date_time[posit_date_time] = \
                (posit_pixel_x, posit_pixel_y, posit_pixel_color)

        return normalized_posits_by_date_time
