import scala.actors.Actor
import scala.actors.Actor.self
import scala.math.log
import scala.collection.immutable.List
import java.util.Random
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ops._
import scala.actors.remote.RemoteActor._
import scala.actors.remote._
import scala.actors.remote.Node
import scala.actors.remote.RemoteActor.{ alive, register }
import scala.actors.OutputChannel
import java.util.Arrays
import scala.util.control.Breaks._


object project3 {
  def main(args: Array[String]): Unit =
    {
      var Nodes = args(0).toInt
      var req = args(1).toInt

      println("------------------------------------------------------------------------")
	if(Nodes>500){      
	println("Since Number of Nodes greater than 500, might take some time")
	}
      println("Number of Nodes    :: "+Nodes)
      println("Number of Requests :: "+req)
      var base = 4 // base is set to 4 
      var len = 6 // total number of digits by which next node to be routed to will be checked
      println("Value of 'b'       :: "+base)
      println("------------------------------------------------------------------------")
      var t: Parent = new Parent(Nodes, base, len, req)
      t.starttop();
    }
}

//Parent Actor to initiate the network and build the topology!

class Parent(Nodes: Int, bas: Int, len: Int, requests: Int) extends Actor {
  var totalNodes: Int = Nodes
  var top: String = null
  var algo: String = null
  var base: Int = bas
  var worker: Array[Worker] = new Array(totalNodes)
  var init: Int = 0
  var endtime: Double = 0
  var starttime: Double = 0
  var term: Int = 0
  var flag: Int = 0
  var req: Int = requests
  var rtable = Array.ofDim[String](len.toInt, base.toInt)
  var started: Boolean = false;
  var global: String = null
  var ref: HashMap[String, Worker] = new HashMap[String, Worker]()
  var rand = List[String]();
  var vall: Array[String] = new Array(4);
  var num_of_hops: Int = 0
  var end: Int = 0
  var sent: Boolean = false
  var total_req: Int = 0
  var f_nodes = totalNodes / 10
  var d_nodes = totalNodes - f_nodes
  def act() {
    loop {
      if (sent && end == 0) {
	var ratio:Double = ((num_of_hops*1.0) / (total_req*1.0))
	var expectRatio:Double =  (math.log(totalNodes) / math.log(16))
	println("Expected value of number of hops :" + expectRatio );
        println("Average number of hops taken     :" + ratio );
	println("Program Terminated.")
        System.exit(1);
      }

      react {
        case (hops: Int, str: String) =>
          end -= 1	
          num_of_hops += hops
    
        case joined: Boolean =>  
          flag = 0
       }
    }
  }

// Method for sending message
  def sendMessages() {
    this.end = rand.length * req;
    this.total_req = end
    for (i <- 0 until rand.length) {
      var child: Worker = ref.getOrElse(rand(i), null)
      if (child != null) {
        child ! (this, req)
        sent = true
      }
    }
  }


  def send() {
    var l = 0
    while (l < 15) {

      var k: Int = 1
      var newNode: Worker = null
      var f: StringBuffer = null
      while (k != 0) {
        f = new StringBuffer();
        for (i <- 0 until len) {
          f.append(vall(new Random().nextInt(vall.length)))
        }
        if (!rand.contains(f.toString())) {
          k = 0
        }
      }

      worker(8) ! (this, 0, f.toString())
      flag = 1
      l += 1
    }
  }

  def add() {

    var l = 0;
    var star: Boolean = false
    var neighbor_node: String = null
    while (l < d_nodes) {

      var k: Int = 1
      var newNode: Worker = null
      var f: StringBuffer = null
      while (k != 0) {
        f = new StringBuffer();
        for (i <- 0 until len) {
          f.append(vall(new Random().nextInt(vall.length)))
        }
        if (!rand.contains(f.toString())) {
        //  trying to join
          newNode = new Worker(f.toString(), this.base, len)
          rand ::= f.toString();
          ref += f.toString() -> newNode
          k = 0
        }
      }

      if (f_nodes >= 2) neighbor_node = rand(f_nodes / 2);

      if (ref.getOrElse(neighbor_node, null) != null) {
             newNode.setRef(ref)
      } 
      newNode.start()
      newNode ! (this, true, neighbor_node)
      flag = 1
      do {
        Thread.sleep(100)
      } while (flag == 1)
      l += 1
    }
  }

//Creating the starttop method here 
  def starttop() {

    var start: String = "start"
    var num: Int = f_nodes
    var link: HashMap[Int, Worker] = new HashMap[Int, Worker]()
    started = true;
    this.start()
    for (i <- 0 until base) {
      vall(i) = i.toString();
    }
    var j = 0
    while (j < num) {
      var f: StringBuffer = new StringBuffer();
      for (i <- 0 until len) {
        f.append(vall(new Random().nextInt(vall.length)))
      }
      if (!rand.contains(f.toString())) {
        rand ::= f.toString();
        j += 1
      }
    }
    
    rand = rand.removeDuplicates
    rand = rand.sortWith((e1, e2) => (e1 < e2))
    
    for (i <- 0 until rand.length) {
      worker(i) = new Worker(rand(i), this.base, len)
      worker(i).start()
      link += i -> worker(i)
      ref += rand(i) -> worker(i)
    }
    var m = 0

    while (m < rand.length) {
      var lowleaf = List[String]();
      lowleaf = rand.slice(math.max(0, m - 4), math.max(1, m));
      var highleaf = List[String]();
      highleaf = rand.slice(math.min(rand.length - 1, m + 1), math.min(rand.length, m + 5));
     
      // Setting lowleaf
      var neghset = List[String]();
      link(m).setlowleaf(lowleaf)

      //setting highleaf
      link(m).sethighleaf(highleaf)
      // setting the reflist
      link(m).setRef(ref)
      m += 1
    }

    var addr: List[String] = rand
    var newls: List[String] = addr
    for (k <- 0 until newls.length) {
      addr = newls
      var map = new HashMap[String, Int]();
      var rtable = Array.ofDim[String](len, base)
      for (i <- 0 until addr.length) {
        map += addr(i) -> i
      }
      var dest: String = addr(k)

      var index: Int = map.getOrElse(dest, 0)
      for (i <- 0 to dest.size - 1) {
        var t_list = List[String]();
        
        addr = addr.sortWith((e1, e2) => (e1 < e2))
        if (i > 0) {
          breakable {
            for (j <- 0 until addr.length) {
              if (addr(j).substring(0, i).toInt > dest.substring(0, i).toInt) {
                break
              }
              var x = addr(j).substring(0, i).r findPrefixMatchOf dest
              if (x.size != 0) {
                t_list ::= addr(j);
        
                if (addr(j) == dest)
                  index = t_list.length - 1;
              }
            }
          }
        }
        if (i != 0 & t_list.length > 0)
          addr = t_list.reverse

        for (j <- 0 until base) {
          var decr: Int = index - 1;
          var incr: Int = index + 1;

          if (decr >= 0 && (addr(decr).charAt(i) - '0') >= j.toInt) {
            while (decr >= 0 && (addr(decr).charAt(i) - '0') > j.toInt) {
              decr = decr - 1;
            }
            if (decr >= 0) {
              rtable(i)(j) = addr(decr)
            } else {
              rtable(i)(j) = null;
            }
          } else if (incr < addr.length && (addr(incr).charAt(i) - '0') <= j.toInt) {

            while (incr < addr.length && (addr(incr).charAt(i) - '0') < j.toInt)
              incr = incr + 1;
            if (incr < addr.length) {   
              rtable(i)(j) = addr(incr)
            } else {
              rtable(i)(j) = null;
            }
          }
        }
      }

      link(k).setRtable(rtable)

    }
    add()
    sendMessages()
  }
}


// defining Node
class Worker(nodeId: String, base: Int, len: Int) extends Actor {
  var node: String = nodeId
  var algo: String = null
  var lowleaf: List[String] = null
  var highleaf: List[String] = null
  var halt: Boolean = false
  var rtable = Array.ofDim[String](len.toInt, base.toInt)
  var refList: Map[String, Worker] = null
  var neghset: List[String] = null
  var resp: Worker = null
  var rout_dest: Worker = null
  var order: Int = 0;
  var send_val = new StringBuffer()
  var send: Parent = null
  var hops: Int = 0
  var parent: Parent = null
  def setNeighSet(neghset: List[String]) {
    this.neghset = neghset;
  }
//Write methods to define low leaf and high leaf as well as routing table
  def setlowleaf(lowleaf: List[String]) {
    this.lowleaf = lowleaf;
  }
  def sethighleaf(highleaf: List[String]) {
    this.highleaf = highleaf;
  }

  def setRtable(rtable: Array[Array[String]]) {
    this.rtable = rtable;
  }
  def getNext(): List[String] = {
    return this.lowleaf;
  }
  def setRef(ref: HashMap[String, Worker]) {
    this.refList = ref;
  }
  def act() {
    var t: Int = 0
    var x: Worker = null
    register(Symbol("worker" + nodeId.toString()), self);
    loop {
      react {
        case ref: HashMap[String, Worker] => this.refList = ref
        case (low: List[String], id: String) =>
          updateLowLeaf(low, id)

        case (high: List[String], id: String, set: Int) =>
          updateHighLeaf(high, id)
        case rtable: Array[Array[String]] => this.rtable = rtable

        case (boss: Parent, hop: Int, message: String) =>
          this.hops = hop
          this.parent = boss
          this.forward(message, false)

        case (boss: Parent, num: Int) =>
          this.parent = boss
          generateMessage(num)
        case (message: String, order: Int, w: Worker) =>
          this.resp = w
          this.update_fwd(message, order)
        case (update: Boolean) =>
          trans()
        case (message: String, w: Worker) =>
          update(message, w)

        case (row: String, order: Int) =>
          this.update(row: String, order)
        case (join: Boolean, newl: String, w: Worker) =>
 	  //joining the workforce
          this.resp = w
          this.update_fwd(newl, 0)
        case (w: Parent, join: Boolean, dest: String) =>
          this.send = w
          startjoin(dest)
      }
    }
  }

  def generateMessage(num: Int) {
    var l = 0
    var vall: Array[String] = new Array(4);
    for (i <- 0 until base) {
      vall(i) = i.toString();
    }

    while (l < num) {
      var k: Int = 1
      var f: StringBuffer = null
      while (k != 0) {
        f = new StringBuffer();
        for (i <- 0 until len) {
          f.append(vall(new Random().nextInt(vall.length)))
        }
        k = 0

      }  	
      this.forward(f.toString(), false)
      l += 1
    }
  }

  def startjoin(dest: String) {
    if (refList != null) {
      this.rout_dest = refList.getOrElse(dest, null)
      if (rout_dest != null) {
        rout_dest ! (true, node, this)
      } 
    } 
  }
//updating low leaf here
  def updateLowLeaf(low: List[String], id: String) {
    var temp = low
    if (node > id && temp.length != 1) {
      temp ::= id
      temp = temp.sortWith((e1, e2) => (e1 < e2))
      temp = temp.drop(1)
      this.lowleaf = temp
    } else {
      this.lowleaf = low
    }
  }
//updating high leaf here
  def updateHighLeaf(high: List[String], id: String) {
    var temp = high
    if (node < id && temp.length != 1) {
      temp ::= id
      temp = temp.sortWith((e1, e2) => (e1 < e2))
      temp = temp.dropRight(1)
      this.highleaf = temp
    } else {
      this.highleaf = high
    }
  }

  def trans() {
    if (refList == null) {
   	// ref lists set to null
    }
    if (lowleaf != null) {
      for (i <- 0 until lowleaf.length) {
        this.rout_dest = refList.getOrElse(lowleaf(i), null)
        if (rout_dest != null) {
          rout_dest ! (node, this)
        }
      }
    }
    if (highleaf != null) {
      for (i <- 0 until highleaf.length) {
        this.rout_dest = refList.getOrElse(highleaf(i), null)
        if (rout_dest != null && highleaf != null) {
          rout_dest ! (node, this)
        }
      }
    }
    if (rtable != null) {
      for (i <- 0 until len) {
        for (j <- 0 until base) {
          this.rout_dest = refList.getOrElse(rtable(i)(j), null)
          if (rout_dest != null && rtable(i)(j) != null && rtable(i)(j) != "null") {
            rout_dest ! (node, this)
          }
        }
      }
    }
    send ! true
  }

  def update(s1: String, w: Worker) {

    if (s1 == null) {
      return
    }
    var src: Int = s1.toInt
    this.refList += s1 -> w

    if (this.lowleaf(0).toInt < src & this.lowleaf(lowleaf.length - 1).toInt > src) {
      lowleaf ::= s1;
      lowleaf = lowleaf.sortWith((e1, e2) => (e1 < e2))
      lowleaf = lowleaf.drop(1)

    } else if (this.highleaf(0).toInt <= src & this.highleaf(highleaf.length - 1).toInt >= src) {
      highleaf ::= s1;
      highleaf = highleaf.sortWith((e1, e2) => (e1 < e2))
      highleaf = highleaf.dropRight(1)
    }

    //lookup in the rtable

    if (rtable != null) {
      var src = this.nodeId
      var row: Int = 0;
      var col: Int = 0;
      var value: Int = s1.toInt
      breakable {
        for (i <- 0 until s1.length()) {
          if (src(i) != s1(i)) {
            break
          } else
            row += 1
        }
      }

      if (row < s1.length) {
        col = s1.charAt(row).toInt - '0';
      }

      var look: String = rtable(row)(col)
      if (look != null && s1 != null && look != "null") {
        if (math.abs(node.toInt - look.toInt) > math.abs(node.toInt - value)) {
          rtable(row)(col) = s1
        }
      }
    }
  }

  def update_fwd(s1: String, order: Int) {

    var src: Int = s1.toInt
    this.order = order
    this.refList += s1 -> this.resp
    var count: Int = 0
    if (rtable != null && order < len) {
      for (j <- 0 until base) {
        if (rtable(order)(j) != null && rtable(order)(j) != "null") {
          count = 1
        }
        send_val.append(rtable(order)(j) + " ");
      }
    }

    if (this.resp != null && count != 0) {
      resp ! (send_val.toString(), order)
    }
    this.order += 1
    this.send_val = new StringBuffer();
    forward(s1, true)
  }

  def update(row: String, order: Int) {
    if (this.rtable != null) {
      var values = row.split(" ");
      for (j <- 0 until base) {
        rtable(order)(j) = values(j)
      }
    }
  }

//Define Forward function
  def forward(s1: String, update: Boolean) {
    var x = new Random()
    var src: Int = s1.toInt
    var close: String = null
    if (lowleaf(0).toInt <= src && highleaf(highleaf.length - 1).toInt >= src) {
      if (this.lowleaf(0).toInt <= src & this.lowleaf(lowleaf.length - 1).toInt >= src) {
        var min: Int = 1000000000;
        if (math.abs(this.node.toInt - src.toInt) <= math.abs(lowleaf(lowleaf.length - 1).toInt - src.toInt)) {
          isDest(update)
          if (!update) {
            parent ! (this.hops, s1)
            return
          }
        }
        breakable {
          for (i <- 0 until lowleaf.length) {

            if (min < math.abs(src.toInt - lowleaf(i).toInt)) {
              close = lowleaf(i - 1);
              break;
            } else {
              close = lowleaf(i)
            }
            min = math.min(min, math.abs(src.toInt - lowleaf(i).toInt))
          }
        }
        if (close != null && math.abs(this.node.toInt - src.toInt) < math.abs(close.toInt - src.toInt)) {
          isDest(update)
          if (!update) {
            parent ! (this.hops, s1)
            return
          } 
        }
      } else if (this.highleaf(0).toInt <= src & this.highleaf(highleaf.length - 1).toInt >= src) {

        var min: Int = 1000000000;
        if (math.abs(this.node.toInt - src.toInt) <= math.abs(highleaf(0).toInt - src.toInt)) {
          isDest(update)
          if (!update) {
            parent ! (this.hops, s1)
            return
          }
        }
        breakable {
          for (i <- 0 until highleaf.length) {

            if (min < math.abs(src.toInt - highleaf(i).toInt)) {
              close = highleaf(i - 1);
              break;
            }
            min = math.min(min, math.abs(src.toInt - highleaf(i).toInt))
          }
        }

        if (close != null && math.abs(this.node.toInt - src.toInt) < math.abs(close.toInt - src.toInt)) {
          isDest(update)
          if (!update) {
            parent ! (this.hops, s1)
            return
          }
        }
        if (close == null) {
          close = highleaf(highleaf.length - 1)
        }
      } else {
        if (this.node.toInt > src.toInt) {

          if ((math.abs(this.node.toInt - src.toInt)) <= math.abs(lowleaf(lowleaf.length - 1).toInt - src.toInt)) {
            isDest(update)
            if (!update) {
              parent ! (this.hops, s1)
              return
            }
          } else
            close = lowleaf(lowleaf.length - 1)

        } else {

          if (math.abs(this.node.toInt - src.toInt) <= math.abs(highleaf(0).toInt - src.toInt)) {
            isDest(update)
            if (!update) {
              parent ! (this.hops, s1)
              return
            }
          } else
            close = highleaf(0)
        }

      }
      if (refList != null && close != null) {
        rout_dest = refList.getOrElse(close, null)
        if (rout_dest != null) {
          if (update) {
            rout_dest ! (s1, order, this.resp)
          } else {
            this.hops += 1
            rout_dest ! (this.parent, hops, s1)
            this.hops = 0
          }
        }
      }
    } //Case 2 Lookup in the routing table
    else if (rtable != null) {
      var src = this.nodeId
      var row: Int = 0;
      var col: Int = 0;
      breakable {
        for (i <- 0 until s1.length()) {
          if (src(i) != s1(i)) {
            break
          } else
            row += 1

        }
      }

      if (row < s1.length) {
        col = s1.charAt(row).toInt - '0';
      }
      var look: String = rtable(row)(col)   
      if (look != null && look != "null") {
        if (refList != null) {
          rout_dest = refList.getOrElse(look, null)
          if (rout_dest != null) {
            if (update) {
              rout_dest ! (s1, order, this.resp)
            } else {
              this.hops += 1
              rout_dest ! (this.parent, hops, s1)
              this.hops = 0
            }
          }
        }
      } else {
      // In case 3
        var dest: String = null;
        
        if (lowleaf.length == 1 && this.lowleaf(0).toInt > s1.toInt) {
          isDest(update)
          if (!update)
            parent ! (this.hops, s1)
        } else if (highleaf.length == 1 && this.highleaf(0).toInt < s1.toInt) {
          isDest(update)
          if (!update)
            parent ! (this.hops, s1)

        } else if (this.lowleaf(0).toInt > s1.toInt) {
          dest = lowleaf(0);
        } else if (highleaf(highleaf.length - 1).toInt < s1.toInt) {
          dest = highleaf(highleaf.length - 1)
        }

        if (refList != null && dest != null) {
          rout_dest = refList.getOrElse(dest, null)
          if (rout_dest != null) {
            if (update) {
              rout_dest ! (s1, order, this.resp)
            } else
              this.hops += 1
            rout_dest ! (this.parent, hops, s1)
            this.hops = 0
          }
        }
      }
    }
    flush()
  }

  def isDest(update: Boolean) {
    if (update && resp != null) {
      resp ! (lowleaf, this.node)
      resp ! (highleaf, this.node, 1)
      resp ! (true)
    }
  }
  def flush() {
    this.order = 0
  }
}
