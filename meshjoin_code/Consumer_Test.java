package test;

import java.sql.*;
import java.time.LocalDate;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer_Test implements Runnable {

    private final BlockingQueue<ConcurrentHashMap<String, String>> transformedQueue;
    private final String jdbcUrl;;
    private final String jdbcUser;
    private final String jdbcPassword;

    public Consumer_Test(BlockingQueue<ConcurrentHashMap<String, String>> transformedQueue, String jdbcUrl, String jdbcUser, String jdbcPassword) {
        this.transformedQueue = transformedQueue;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
    }

    @Override
    public void run() {
        System.out.println("In Thread Consumer");
        while (true) {
            try {
                
                ConcurrentHashMap<String, String> dataMap = transformedQueue.take();
                processAndLoadData(dataMap);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Consumer thread interrupted");
            }
        }
    }

    private void processAndLoadData(ConcurrentHashMap<String, String> dataMap) {
        for (String compositeKey : dataMap.keySet()) {
            String data = dataMap.get(compositeKey);
            String[] fields = data.split(",");
            if (fields.length < 14) {
                System.err.println("Invalid data format for key: " + compositeKey + ", Data: " + data);
                continue;
            }

            String customerId = fields[0];
            String customerName = fields[1];
            String gender = fields[2];
            String productId = fields[3];
            String productName = fields[4];
            String productPrice = fields[5];
            String supplierId = fields[6];
            String supplierName = fields[7];
            String storeId = fields[8];
            String storeName = fields[9];
            String orderId = fields[10];
            String orderDateTime = fields[11]; 
            String quantity = fields[13];
            String sale = calculateSale(fields[5], fields[13]);

            if (orderDateTime == null || orderDateTime.isEmpty()) {
                orderDateTime = "1970-01-01 00:00:00";
            }

            String[] orderDateTimeParts = orderDateTime.split(" ");
            String orderDate = orderDateTimeParts[0];
            String orderTime = orderDateTimeParts.length > 1 ? orderDateTimeParts[1] : "00:00:00";

            addToTimeDimension(orderDateTime);
            int timeId = getTimeID(orderDate);
            if (timeId == -1) {
                //System.err.println("Failed to fetch time_ID for orderDate: " + orderDate);
                continue;
            }

            // Load to database
            loadDataToDatabase(customerId, customerName, gender, productId, productName, productPrice,
                    supplierId, supplierName, storeId, storeName, orderId, timeId, quantity);

            // Debugging output
            System.out.println("Processed Data for Order ID: " + orderId);
        }
    }

    private String calculateSale(String price, String quantity) {
        try {
            double priceValue = Double.parseDouble(price.replace("$", ""));
            int quantityValue = Integer.parseInt(quantity);
            return String.format("%.2f$", priceValue * quantityValue);
        } catch (NumberFormatException e) {
            System.err.println("Error calculating sale for price: " + price + ", quantity: " + quantity);
            return "Invalid sale value";
        }
    }
    
    private boolean isWeekday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        // Sunday = 1, Saturday = 7, so weekdays are 2-6 (Monday to Friday)
        return dayOfWeek >= Calendar.MONDAY && dayOfWeek <= Calendar.FRIDAY;
    }
    
    private void addToTimeDimension(String orderDateTime) {
        SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy H:mm");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = null;
        try {
            date = inputFormat.parse(orderDateTime);
        } catch (ParseException e) {
            return;
        }

        String orderDate = outputFormat.format(date).split(" ")[0];
        String[] dateParts = orderDate.split("-");
        int year = Integer.parseInt(dateParts[0]);
        int month = Integer.parseInt(dateParts[1]);
        int day = Integer.parseInt(dateParts[2]);
        String season = getSeason(month);
        int quarter = (month - 1) / 3 + 1;

        boolean isWeekday = isWeekday(date);
        java.sql.Date sqlDate = java.sql.Date.valueOf(orderDate);
        int timeId = getTimeID(orderDate);
        if (timeId == -1) {
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
                String sql = "INSERT INTO Time (ORDER_DATE, DAY, MONTH, SEASON, QUARTER, YEAR, WEEKDAY) VALUES (?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setDate(1, sqlDate);
                pstmt.setInt(2, day);
                pstmt.setInt(3, month);
                pstmt.setString(4, season);
                pstmt.setInt(5, quarter);
                pstmt.setInt(6, year);
                pstmt.setBoolean(7, isWeekday);
                pstmt.executeUpdate();
            } catch (SQLException e) {
            }
        }
    }


    private int getTimeID(String orderDate) {
        int timeId = -1;
        String selectQuery = "SELECT time_ID FROM Time WHERE order_date = ?";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
            SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");

            Date parsedDate = inputFormat.parse(orderDate);
            String formattedDate = outputFormat.format(parsedDate);  // Convert to YYYY-MM-DD format

            try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery)) {
                preparedStatement.setString(1, formattedDate);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        timeId = resultSet.getInt("time_ID");
                    }
                }
            }
        } catch (SQLException | ParseException e) {
            // Log a minimal error message
            //System.err.println("Error fetching time_ID for order_date: " + orderDate);
        }

        return timeId;
    }




    private String getSeason(int month) {
        switch (month) {
            case 12:
            case 1:
            case 2:
                return "Winter";
            case 3:
            case 4:
            case 5:
                return "Spring";
            case 6:
            case 7:
            case 8:
                return "Summer";
            case 9:
            case 10:
            case 11:
                return "Autumn";
            default:
                return "Unknown";
        }
    }

    private void loadDataToDatabase(String customerId, String customerName, String gender, String productId, 
            String productName, String productPrice, String supplierId, String supplierName, 
            String storeId, String storeName, String orderId, int timeId, String quantity) {
        System.out.println("Connecting to database...");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
            System.out.println("Connected to database successfully!");

           /* String checkOrderQuery = "SELECT COUNT(*) FROM Sales WHERE ORDER_ID = ?";
            try (PreparedStatement checkStmt = connection.prepareStatement(checkOrderQuery)) {
                checkStmt.setString(1, orderId);
                try (ResultSet rs = checkStmt.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        //System.out.println("Duplicate Order ID found: " + orderId);
                        return;  // Skip inserting the duplicate order
                    }
                }
            }*/

            // Populate Dimensions
            insertIntoCustomers(connection, customerId, customerName, gender);
            insertIntoProducts(connection, productId, productName, productPrice);
            insertIntoSuppliers(connection, supplierId, supplierName);
            insertIntoStores(connection, storeId, storeName);

            String insertFactTableQuery = "INSERT INTO Sales (ORDER_ID, PRODUCT_ID, CUSTOMER_ID, STORE_ID, SUPPLIER_ID, " +
                      "TIME_ID, QUANTITY, PRODUCT_PRICE, SALE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertFactTableQuery)) {
                preparedStatement.setString(1, orderId);
                preparedStatement.setString(2, productId);
                preparedStatement.setString(3, customerId);
                preparedStatement.setString(4, storeId);
                preparedStatement.setString(5, supplierId);
                preparedStatement.setInt(6, timeId);
                preparedStatement.setInt(7, Integer.parseInt(quantity));
                preparedStatement.setDouble(8, Double.parseDouble(productPrice.replace("$", "")));
                preparedStatement.setDouble(9, Integer.parseInt(quantity) * Double.parseDouble(productPrice.replace("$", "")));

                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("Database operation failed!");
            e.printStackTrace();
        }
    }

private void insertIntoCustomers(Connection connection, String customerId, String customerName, String gender) {
String insertCustomerQuery = "INSERT INTO Customers (CUSTOMER_ID, CUSTOMER_NAME, GENDER) VALUES (?, ?, ?) " +
              "ON DUPLICATE KEY UPDATE CUSTOMER_NAME = VALUES(CUSTOMER_NAME), GENDER = VALUES(GENDER)";
	try (PreparedStatement preparedStatement = connection.prepareStatement(insertCustomerQuery)) {
	preparedStatement.setInt(1, Integer.parseInt(customerId));
	preparedStatement.setString(2, customerName);
	preparedStatement.setString(3, gender);
	preparedStatement.executeUpdate();
	} catch (SQLException e) {
	System.err.println("Failed to insert/update customer: " + e.getMessage());
	}
}

private void insertIntoProducts(Connection connection, String productId, String productName, String productPrice) {
String insertProductQuery = "INSERT INTO Products (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE) VALUES (?, ?, ?) " +
             "ON DUPLICATE KEY UPDATE PRODUCT_NAME = VALUES(PRODUCT_NAME), PRODUCT_PRICE = VALUES(PRODUCT_PRICE)";
	try (PreparedStatement preparedStatement = connection.prepareStatement(insertProductQuery)) {
	preparedStatement.setInt(1, Integer.parseInt(productId));
	preparedStatement.setString(2, productName);
	preparedStatement.setDouble(3, Double.parseDouble(productPrice.replace("$", "")));
	preparedStatement.executeUpdate();
	} catch (SQLException e) {
	System.err.println("Failed to insert/update product: " + e.getMessage());
	}
}

private void insertIntoSuppliers(Connection connection, String supplierId, String supplierName) {
String insertSupplierQuery = "INSERT INTO Suppliers (SUPPLIER_ID, SUPPLIER_NAME) VALUES (?, ?) " +
              "ON DUPLICATE KEY UPDATE SUPPLIER_NAME = VALUES(SUPPLIER_NAME)";
	try (PreparedStatement preparedStatement = connection.prepareStatement(insertSupplierQuery)) {
	preparedStatement.setInt(1, Integer.parseInt(supplierId));
	preparedStatement.setString(2, supplierName);
	preparedStatement.executeUpdate();
	} catch (SQLException e) {
	System.err.println("Failed to insert/update supplier: " + e.getMessage());
	}
}

private void insertIntoStores(Connection connection, String storeId, String storeName) {
String insertStoreQuery = "INSERT INTO Stores (STORE_ID, STORE_NAME) VALUES (?, ?) " +
           "ON DUPLICATE KEY UPDATE STORE_NAME = VALUES(STORE_NAME)";
	try (PreparedStatement preparedStatement = connection.prepareStatement(insertStoreQuery)) {
	preparedStatement.setInt(1, Integer.parseInt(storeId));
	preparedStatement.setString(2, storeName);
	preparedStatement.executeUpdate();
	} catch (SQLException e) {
	System.err.println("Failed to insert/update store: " + e.getMessage());
	}
}

}
