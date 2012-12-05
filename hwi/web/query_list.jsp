<%--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
--%>
<%@ page import="org.apache.hadoop.hive.hwi.*"%>
<%@page errorPage="error_page.jsp"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="javax.jdo.JDOHelper"%>
<%@page import="java.util.List"%>
<%
HiveConf hiveConf = new HiveConf(SessionState.class);
QueryStore qs = new QueryStore(hiveConf);
           
// Object mquery = JDOHelper.getObjectId(qs.getQuery(1));

List<MQuery> mquerys = qs.getQuerys();

%>

<!DOCTYPE html>
<html>
<head>
<title>Query List</title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span4">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span4 -->
			<div class="span8">

				<h2>Query List</h2>

				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Query</th>
							<th>Status</th>
							<th>Error</th>
							<th>Action</th>
						</tr>
					</thead>
					<tbody>
						<%-- if ( hs.findAllSessionsForUser(auth)!=null){ --%>
						<% for (MQuery item: mquerys ) { %>
						<tr>
							<td><%= item.getName() %></td>
							<td><%= item.getQuery() %></td>
							<td><%= item.getStatus() %></td>
							<td><%= item.getErrorMsg() %></td>
							<td><a href="/hwi/query_manage.jsp?id=<%= item.getId() %>">Manager</a></td>
						</tr>
						<% } %>
						<%-- } --%>
					</tbody>
				</table>
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>

