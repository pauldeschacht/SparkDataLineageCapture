package io.nomad48.datective.flow

object FlowToDot {
  def linesToDot(lines: List[String]): String = {
    lines.mkString("| ")
  }
  def nodeToDot(node: Node): String = {
    val content = linesToDot(node.lines)
    s""""${node.id}" [ label = "<f0> ${node.name} | $content "; shape = "record"; ];""".stripMargin
  }
  def relationToDot(relation: Relation): String = {
    s""""${relation.source}":<f0> -> "${relation.destination}":<f0> [ id=${relation.id} ];""".stripMargin
  }
  def graphToDot(graph: List[GraphElement]): String = {
    val content: String = graph
      .map {
        case node: Node         => nodeToDot(node)
        case relation: Relation => relationToDot(relation)
      }
      .mkString("\n")

    s"""digraph g {
       |graph [ rankdir = "LR" ];
       |node [ fontsize = "16"; shape = "ellipse" ];
       |edge [];
       |$content
       |}
       |""".stripMargin
  }
}
