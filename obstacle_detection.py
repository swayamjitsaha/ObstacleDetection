#!/usr/bin/python3
import rospy
import math
from queue import Queue
import numpy as np
from sensor_msgs.msg import PointCloud2, PointField
import time
from visualization_msgs.msg import Marker
from geometry_msgs.msg import Point

# utility function to publish a point cloud ROS message
def publish_cloud(cloud, timestamp, colors=None):
    pcMsg.header.seq = count_msg
    pcMsg.header.stamp = timestamp
    if colors is None:
        pcMsg.fields = [
            PointField('x',0,PointField.FLOAT32,1),
            PointField('y',4,PointField.FLOAT32,1),
            PointField('z',8,PointField.FLOAT32,1),
        ]
        pcMsg.point_step = 12
    else:
        pcMsg.fields = [
            PointField('x',0,PointField.FLOAT32,1),
            PointField('y',4,PointField.FLOAT32,1),
            PointField('z',8,PointField.FLOAT32,1),
            PointField('rgb',12,PointField.INT32,1),
        ]
        pcMsg.point_step = 16
    pcMsg.width = len(cloud)
    pcMsg.row_step = pcMsg.width * pcMsg.point_step
    if colors is None:
        pcMsg.data = cloud.tobytes()
    else:
        color_int = colors[:,0] << 16 | colors[:,1] << 8 | colors[:,2]
        combined = np.rec.fromarrays([cloud[:,0], cloud[:,1], cloud[:,2], color_int], names='x,y,z,c')
        pcMsg.data = combined.tobytes()
    pubCloud.publish(pcMsg)

# utility function to publish bounding boxes as a ROS message
def publish_boxes(boxes, timestamp):
    boxMsg.header.stamp = timestamp
    boxMsg.points = []
    for box in boxes:
        p1 = Point(*box[0,:])
        p2 = Point(*box[1,:])
        p3 = Point(*box[2,:])
        p4 = Point(*box[3,:])
        p5 = Point(*box[4,:])
        p6 = Point(*box[5,:])
        p7 = Point(*box[6,:])
        p8 = Point(*box[7,:])
        boxMsg.points.extend([p1,p2,p1,p3,p2,p4,p3,p4,p1,p5,p2,p6,p3,p7,p4,p8,p5,p6,p5,p7,p6,p8,p7,p8])
    pubBoxes.publish(boxMsg)

# TODO: implementation function to filter out points with Z value below a specified threshold 
def filter_ground(cloud, ground_level=0):
    
    mask=(cloud[:,2]>ground_level) ######## Updation Starts from Line 59. All the Todo functions needs to be changed actually
    return cloud[mask]
    
# TODO: implementation function to filter out points further than a specified distance
def filter_by_distance(cloud, distance=10):
    
    x=np.square(cloud[:,0])        ######This one
    y=np.square(cloud[:,1])
    resultant=np.sqrt(x+y)
    resultant=(resultant<distance)
    return cloud[resultant]

# TODO: implementation function to perform Euclidean clustering at a specified threshold in meters
def euclidean_clustering(cloud, threshold=0.5):
    cluster_labels = np.zeros(len(cloud), dtype=int)   #############This one
    
    arr_queue= Queue(maxsize=0)
    m=1
    for i in range(len(cloud)):
    	if cluster_labels[i]!=0:
    		continue
    	
    	arr_queue.put(cloud[i])
    	cluster_labels[i]=m
    	while not arr_queue.empty():
    		
    		curr=arr_queue.get()
    		x=np.square(curr[0]-cloud[:,0])
    		
    		y=np.square(curr[1]-cloud[:,1])
    		
    		z=np.square(curr[2]-cloud[:,2])
    		
    		resultant=np.sqrt(x+y+z)
    		
    		neighbours=(0<resultant)&(resultant<threshold)
    		
    		for i in range(len(neighbours)):
    			if neighbours[i] and cluster_labels[i]==0:
    				arr_queue.put(cloud[i])
    				cluster_labels[i]=m
    				
    	arr_queue=Queue(maxsize=0)
    	m+=1
    	
    return cluster_labels

# TODO: (extra credit) implementation function to perform Euclidean clustering with lower computation time
def euclidean_clustering_accelerated(cloud, threshold=0.5):
    cluster_labels = np.zeros(len(cloud), dtype=int)      ##########This one
    return cluster_labels

# TODO: (extra credit) implementation function to filter clusters by number of points
def filter_clusters(cluster_labels, min_num_points=100):
    for i in range(1,np.amax(cluster_labels)+1):               ##########This one
    	if (cluster_labels==i).sum()<min_num_points:
    		cluster_labels[cluster_labels==i]=0
    		cluster_labels[cluster_labels>i]-=1
    
    return cluster_labels

# TODO: implementation function to compute bounding boxes from cluster labels
def get_bounding_boxes(cloud, cluster_labels):
    boxes = []                                         ##############This one is the last function typically to change
    for i in range(1,np.amax(cluster_labels)+1):
	    mask=cluster_labels==i
	    curr=cloud[mask]
	    
	    
	    xmin=np.amin(curr[:,0])
	    xmax=np.amax(curr[:,0])
	    ymin=np.amin(curr[:,1])
	    ymax=np.amax(curr[:,1])
	    zmin=np.amin(curr[:,2])
	    zmax=np.amax(curr[:,2])
	    
	    
	    boxes.append(np.array([
		[xmin,ymin,zmin],
		[xmin,ymin,zmax],
		[xmin,ymax,zmin],
		[xmin,ymax,zmax],
		[xmax,ymin,zmin],
		[xmax,ymin,zmax],
		[xmax,ymax,zmin],
		[xmax,ymax,zmax],
	    ]))

    return boxes

# callback function to subscribe to the ROS point cloud message input
def point_cloud_callback(msg):
    global count_msg
    start_time = time.time()
    input_cloud = np.frombuffer(msg.data, dtype=np.float32).reshape(-1, 4)

    # Filter out ground points
    filtered_cloud = filter_ground(input_cloud, -1.2)

    # Filter out points that are out of range
    filtered_cloud = filter_by_distance(filtered_cloud, 10)

    # Determine cluster labels
    cluster_labels = euclidean_clustering(filtered_cloud[:, :3])

    # Filter out clusters that are too small
    cluster_labels = filter_clusters(cluster_labels, 100)

    # Set a unique color for each cluster
    colors = np.zeros((len(filtered_cloud), 3), dtype=np.int32)
    for i in range(cluster_labels.min(), cluster_labels.max()+1):
        cluster_mask = cluster_labels == i
        colors[cluster_mask] = np.random.randint(0,255,3)

    # Compute a bounding box for each cluster
    boxes = get_bounding_boxes(filtered_cloud, cluster_labels)
    publish_boxes(boxes, msg.header.stamp)

    publish_cloud(filtered_cloud[:, :3], msg.header.stamp, colors)
    count_msg += 1
    end_time = time.time()
    print("Message %d: Processed %d points %d clusters in %.3fs" % (count_msg, len(input_cloud), len(set(cluster_labels) - set([0])), end_time - start_time))


rospy.init_node('obstacle_detection', anonymous=True)
count_msg = 0
rospy.Subscriber("/kitti/velo/pointcloud", PointCloud2, point_cloud_callback)

# initialize the output ROS messages
pcMsg = PointCloud2()
pcMsg.header.frame_id = 'velo_link'
pcMsg.height = 1
pcMsg.is_bigendian = False
pcMsg.is_dense = True
pubCloud = rospy.Publisher('output_cloud', PointCloud2, queue_size=1)

boxMsg = Marker()
boxMsg.header.frame_id = "velo_link";
boxMsg.type = Marker.LINE_LIST;
boxMsg.lifetime = rospy.Duration();
boxMsg.color.a = 1.0;
boxMsg.action = Marker.ADD;
boxMsg.scale.x = 0.05;
boxMsg.pose.orientation.w = 1.0;
boxMsg.id = 0;
boxMsg.color.r = 1.0;
boxMsg.color.g = 1.0;
boxMsg.color.b = 0;
pubBoxes = rospy.Publisher('boxes', Marker, queue_size=1)

rospy.spin()