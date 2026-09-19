from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("DegreeOfSeparation")
sc = SparkContext(conf=conf)

startSuperHeroId = 5306
targetSuperHeroId = 38

hitTarget = sc.accumulator(0)


def linesToNodes(line):
    fields = line.split()

    superHeroId = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]
    color = "White"
    distance = 9999

    if superHeroId == startSuperHeroId:
        color = "Gray"
        distance = 0

    return (superHeroId, (connections, distance, color))


def bfsMap(node):
    superHeroId = node[0]
    connections = node[1][0]
    distance = node[1][1]
    color = node[1][2]

    connectionsExtended = []

    if color == "Gray":
        for connection in connections:
            if connection == targetSuperHeroId:
                hitTarget.add(1)

            connectionsExtended.append((connection, ([], distance + 1, "Gray")))

        color = "Black"

    connectionsExtended.append((superHeroId, (connections, distance, color)))

    return connectionsExtended


def bfsReduce(lNode, rNode):
    connections = [*lNode[0], *rNode[0]]
    distance = min(lNode[1], rNode[1])
    lColor = lNode[2]
    rColor = rNode[2]
    color = ""

    if lColor == "White" and rColor in ("Black", "Gray"):
        color = rColor

    if rColor == "White" and lColor in ("Black", "Gray"):
        color = lColor

    if lColor == "Gray" and rColor == "Black":
        color = rColor

    if rColor == "Gray" and lColor == "Black":
        color = lColor

    return (connections, distance, color)


lines = sc.textFile("./DataFiles/MarvelGraph")
superHeroRDD = lines.map(linesToNodes)

for i in range(0, 10):
    print(f"Itteration {i + 1}")

    mapped = superHeroRDD.flatMap(bfsMap)

    print(f"Processing {mapped.count()} values")  # acction function

    if hitTarget.value > 0:
        print(f"Hit the target character! from {hitTarget.value} direction(s).")
        break

    superHeroRDD = mapped.reduceByKey(bfsReduce)
