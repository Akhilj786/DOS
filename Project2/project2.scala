import akka.actor._
import scala.util.Random
import scala.collection.mutable;
import scala.collection.immutable.Queue

object global {
  var map = mutable.Map.empty[Int, List[Int]];
  var ind_count = mutable.Map.empty[Int, Int];
}

object globalTime {
  var b: Long = 0;
}

object project2 extends App {
  var topology: String = args(1).toString;
  var numNodes: Int = args(0).toInt;
  var algorithm: String = args(2).toString;

  println("Number of Nodes :: " + numNodes)
  println("Algorithm       :: " + algorithm)
  println("Topology        :: " + topology)
  println()

  val system1 = ActorSystem("FirstSystem");
  var GossipMaster = system1.actorOf(Props(new GossipMaster(numNodes, algorithm)), "gossipMaster");
  GossipMaster ! (topology, numNodes);
}

class GossipMaster(numNodes: Int, algorithm: String) extends Actor {
  var keepTrack = Array.fill[Double](numNodes)(0)
  var count: Int = 0;

  def receive = {

    case (topology: String, numNod: Int) => {

      //Creating Actor system and array of nodes to create corresponding topology
      val system1 = ActorSystem("secSystem");
      var z = Array.ofDim[ActorRef](numNodes)

      //switch case for topology match
      topology match {
        case "line" =>
          compute_LineMap(numNodes);
        case "full" =>
          compute_Flood(numNodes);
        case "2D" =>
          compute_2dMap(numNodes);
        case "imp2D" =>
          compute_imperfect2dMap(numNodes);
      }

      //Initiating all Nodes
      for (i <- 0 to numNodes - 1) {
        var len: Int = show(global.map.get(i)).asInstanceOf[Int];
        z(i) = system1.actorOf(Props(new Node(i, len, self)), name = "Node" + i);
      }

      //Algorithm selection
      globalTime.b = System.currentTimeMillis();
      if (algorithm == "gossip") {
        z(0) ! ("Hello");
      } else if (algorithm == "push-sum") {
        z(0) ! (0.0,1.0)
      }

    }

    case (done: String) => {
      count += 1;
      if (count == numNodes) {
        println("Message reached all nodes")

        println("Time taken is " + (System.currentTimeMillis() - globalTime.b));
        println("-----Exiting------")
        sys.exit();
      }
    }

    case (pushDone: String, nodeId: Int, ratio: Double) => {
      count += 1;
      keepTrack(nodeId) = ratio;
      var threshold:Int=(0.95* numNodes).toInt;
      if (count == numNodes) {
        println("Master Done");
	var time = (System.currentTimeMillis() - globalTime.b)
        
        for (aj <- 0 to numNodes - 1) {
          println("Node " + aj + " ratio is " + keepTrack(aj))
        }
	println("Time taken is " + time);
        println("-----Exiting------")
        sys.exit();
      }
    }
  }

  def compute_LineMap(no_of_node: Int) {
    for (i <- 0 to no_of_node - 1) {
      global.ind_count(i) = 0;
      if (i == 0)
        global.map(i) = List(i + 1);
      else if (i == no_of_node - 1)
        global.map(i) = List(i - 1);
      else
        global.map(i) = List(i - 1, i + 1);
      //println(global.map(i))
    }
  }

  def compute_Flood(no_of_node: Int) {
    for (i <- 0 to no_of_node - 1) {
      global.ind_count(i) = 0;
      var ls: List[Int] = List();
      for (j <- 0 to no_of_node - 1) {
        if (i != j)
          ls = j +: ls
      }
      global.map(i) = ls;
      //println(global.map(i))
    }
  }

  def compute_2dMap(no_of_node: Int) {
    var close_sqrt: Int = Math.ceil(Math.sqrt(no_of_node)).toInt;
    for (i: Int <- 0 to no_of_node - 1) {
      global.ind_count(i) = 0;
      var ls: List[Int] = List();
      if ((i - close_sqrt) < 0) {}
      else {
        ls = (i - close_sqrt) +: ls;
      }
      if ((close_sqrt + i) > no_of_node - 1) {}
      else {
        ls = (close_sqrt + i) +: ls;
      }
      if (i % close_sqrt == 0) {}
      else {
        ls = (i - 1) +: ls
      }
      if ((i + 1) % close_sqrt == 0 || (i + 1) >= no_of_node) {}
      else {
        ls = (i + 1) +: ls
      }
      global.map(i) = ls;
    }
  }

  def compute_imperfect2dMap(no_of_node: Int) {
    var close_sqrt: Int = Math.ceil(Math.sqrt(no_of_node)).toInt;
    for (i: Int <- 0 to no_of_node - 1) {
      global.ind_count(i) = 0;
      var ls: List[Int] = List();
      if ((i - close_sqrt) < 0) {}
      else {
        ls = (i - close_sqrt) +: ls;
      }
      if ((close_sqrt + i) > no_of_node - 1) {}
      else {
        ls = (close_sqrt + i) +: ls;
      }
      if (i % close_sqrt == 0) {}
      else {
        ls = (i - 1) +: ls
      }
      if ((i + 1) % close_sqrt == 0 || (i + 1) >= no_of_node) {}
      else {
        ls = (i + 1) +: ls
      }

      var macG = Random.nextInt(no_of_node)

      while (ls.contains(macG)) {
        macG = Random.nextInt(no_of_node)
      }
      ls = macG +: ls;
      global.map(i) = ls;
    }
  }

  def show(x: Option[List[Int]]) = x match {
    case Some(s) => s.length;
    case None => "?"
  }
}

class Node(n: Int, len: Int, Master: ActorRef) extends Actor {
  var count = 0;
  var s: Double = n;
  var w: Double = 1;
  var pushCount = 0;
  var pushArray: Array[Double] = Array.fill(3)(100)
  var slidingWindow = new scala.collection.mutable.Queue[Double]()
	slidingWindow.enqueue(10000)
	slidingWindow.enqueue(10000)
	
  var que = collection.immutable.Queue()
  var flag = 0
  def receive = {
    case (rec: String) =>
      {
        if (count < 10) {
          count += 1;
          global.ind_count(n) = global.ind_count(n) + 1;

          if (count == 10) {
            Master ! "Done";
          }

          var random: Int = Random.nextInt(len);
          var neighbour: Int = global.map(n).apply(random);

          context.actorSelection("../Node" + neighbour) ! "Hello";
          context.actorSelection("../Node" + n) ! "Hello";
        } else sender ! ("done", 1);

      }

    case (done: String, faltu: Int) => {
      var random: Int = Random.nextInt(len);
      var neighbour: Int = global.map(n).apply(random);
      context.actorSelection("../Node" + neighbour) ! "Hello";
    }
    case (sum: Double, weight: Double) => {
      if (flag == 0) {
        s += sum;
        w += weight;
        var ratio = (s / w)
        s = s / 2;
        w = w / 2;
    
	slidingWindow.enqueue(ratio)
	
        //=============================================================================================
     if ((Math.abs(slidingWindow.head - slidingWindow.last)) <= 0.001){
          flag = 1;
          Master ! ("PushDone", n, ratio);
        }
		slidingWindow.dequeue
        var random1: Int = Random.nextInt(len);
        var neighbour1: Int = global.map(n).apply(random1);
        context.actorSelection("../Node" + neighbour1) ! (s, w);
        context.actorSelection("../Node" + n) ! (s, w);

      } else sender ! ("done", sum, weight)

    }
    case (done1: String, sum1: Double, weight1: Double) => {
      //println("in push done wala case")
      var random1: Int = Random.nextInt(len);
      var neighbour1: Int = global.map(n).apply(random1);
      context.actorSelection("../Node" + neighbour1) ! (sum1, weight1);

    }

  }
}

