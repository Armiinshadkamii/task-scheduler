use crossbeam_channel::bounded;
use std::thread;
use std::time::Duration;

fn main() {
    // Create a bounded channel with a capacity of 2.
    let (sender, receiver) = bounded::<String>(2);

    // Clone handles for the producers and workers.
    let sender2 = sender.clone();
    let receiver1 = receiver.clone();
    let receiver2 = receiver.clone();

    // Producer 1
    let producer1 = thread::spawn(move || {
        let tasks = vec![
            "Process shipment",
            "Generate invoice",
            "Update tracking",
            "Send notification",
            "Validate payment",
            "Calculate shipping cost",
        ];

        for task in tasks {
            println!("Producer 1 trying to submit: {}", task);

            sender.send(String::from(task)).unwrap();

            println!("Producer 1 successfully submitted: {}", task);
        }
    });

    // Producer 2
    let producer2 = thread::spawn(move || {
        let tasks = vec!["Process payment", "Verify transaction"];

        for task in tasks {
            println!("Producer 2 trying to submit: {}", task);

            sender2.send(String::from(task)).unwrap();

            println!("Producer 2 successfully submitted: {}", task);
        }
    });

    // Worker 1
    let worker1 = thread::spawn(move || {
        while let Ok(task) = receiver1.recv() {
            println!("Worker 1 received: {}", task);

            // Simulate task processing.
            thread::sleep(Duration::from_secs(4));
        }

        println!("Worker 1 shutting down.");
    });

    // Worker 2
    let worker2 = thread::spawn(move || {
        while let Ok(task) = receiver2.recv() {
            println!("Worker 2 received: {}", task);

            // Simulate task processing.
            thread::sleep(Duration::from_secs(2));
        }

        println!("Worker 2 shutting down.");
    });

    // The original receiver is no longer needed.
    drop(receiver);

    // Wait for producers to finish.
    producer1.join().unwrap();
    producer2.join().unwrap();

    // Wait for workers to consume remaining tasks and exit.
    worker1.join().unwrap();
    worker2.join().unwrap();

    println!("Scheduler shut down successfully.");
}
