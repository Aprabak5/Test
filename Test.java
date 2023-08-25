import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@EnableScheduling
public class SpringBootApp {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootApp.class, args);
    }
}

@Service
class DataPollingService {
    private final JdbcTemplate jdbcTemplate;

    public DataPollingService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Scheduled(fixedRate = 5000) // Poll every 5 seconds
    public void pollAndInsertData() {
        List<Map<String, Object>> polledData = jdbcTemplate.queryForList("SELECT id, json_data FROM polled_table WHERE ingested = 0");

        for (Map<String, Object> row : polledData) {
            Long id = (Long) row.get("id");
            String jsonData = (String) row.get("json_data");

            // Deserialize JSON and insert into normalized table
            // Your deserialization and insertion logic here...

            // Update ingested flag
            jdbcTemplate.update("UPDATE polled_table SET ingested = 1 WHERE id = ?", id);
        }
    }
}
