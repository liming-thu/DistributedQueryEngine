<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Index.aspx.cs" Inherits="DistributedQueryService.Index" %>

<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <script type="text/javascript" src="jquery-2.1.4.min.js"></script>
    <script type="text/javascript" src="joint.min.js"></script>
    <script type="text/javascript" src="joint.layout.DirectedGraph.min.js"></script>
    <script type="text/javascript" src="index.js"></script>
    <link rel="stylesheet" type="text/css" href="joint.min.css" />
    <title>Distributed Query Engine</title>
</head>
<body>
<p><textarea id="sql" rows="12" cols="60" style="resize: none">
    </textarea><br>
    <button id="summit">查询</button></p>
<p id="alg-tree"></p>
<p id="optimized-tree"></p>
</body>
</html>
