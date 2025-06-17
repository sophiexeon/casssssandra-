import uuid
import datetime
from datetime import timezone
import os
from containers import CassandraReservationSystem

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def display_header(title):
    """Display a formatted header"""
    clear_screen()
    print("\n" + "="*50)
    print(f"{title}")
    print("="*50)

def wait_for_user():
    """Wait for user to press Enter to continue"""
    input("\nPress Enter to continue...")

def main_menu(reservation_system):
    """Main menu for the reservation system"""
    while True:
        display_header("✈️  FLIGHT RESERVATION SYSTEM - MAIN MENU")
        print("1. User Management")
        print("2. Flight Management")
        print("3. Reservation Management")
        print("4. Airport Worker View")  # New option
        print("5. Run Stress Tests")
        print("6. Exit")
        print("-"*50)
        
        choice = input("Enter your choice (1-6): ")
        
        if choice == '1':
            user_menu(reservation_system)
        elif choice == '2':
            flight_menu(reservation_system)
        elif choice == '3':
            reservation_menu(reservation_system)
        elif choice == '4':
            airport_worker_menu(reservation_system)  # New menu
        elif choice == '5':
            stress_test_menu(reservation_system)
        elif choice == '6':
            print("\nExiting the system. Goodbye!")
            reservation_system.close()
            break
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def user_menu(reservation_system):
    while True:
        display_header("👤 USER MANAGEMENT")
        print("1. Create New User")
        print("2. View User Reservations")
        print("3. Back to Main Menu")
        print("-"*50)
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            username = input("\nEnter username: ")
            email = input("Enter email: ")
            user_id = reservation_system.create_user(username, email)
            if user_id:
                print(f"\n✅ User created successfully!")
                print(f"👉 Your user ID is: {user_id}")
                print("⚠️  Please save this ID for making reservations.")
            wait_for_user()
                
        elif choice == '2':
            user_id_str = input("\nEnter user ID: ")
            try:
                user_id = uuid.UUID(user_id_str)
                reservation_system.list_user_reservations(user_id)
                wait_for_user()
            except ValueError:
                print("❌ Invalid user ID format. Please try again.")
                wait_for_user()
                
        elif choice == '3':
            return
        
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def flight_menu(reservation_system):
    while True:
        display_header("✈️ FLIGHT MANAGEMENT")
        print("1. Create New Flight")
        print("2. View Flight Status")
        print("3. Back to Main Menu")
        print("-"*50)
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            origin = input("\nEnter origin city: ")
            destination = input("Enter destination city: ")
            
            try:
                days_ahead = int(input("Departure in how many days from now (1-7): ") or "1")
                flight_hours = float(input("Flight duration in hours (1-8): ") or "2")
                seats = int(input("Total seats on flight (10-50): ") or "20")  # Reduced default from unlimited to 20
                
                # Ensure reasonable limits for assignment demonstration
                days_ahead = max(1, min(days_ahead, 7))
                flight_hours = max(0.5, min(flight_hours, 12))
                seats = max(5, min(seats, 100))  # Between 5 and 100 seats
                
                departure = datetime.datetime.now(timezone.utc) + datetime.timedelta(days=days_ahead)
                arrival = departure + datetime.timedelta(hours=flight_hours)
                
                flight_id = reservation_system.create_flight(origin, destination, departure, arrival, seats)
                if flight_id:
                    print(f"\n✅ Flight created successfully!")
                    print(f"👉 Flight ID: {flight_id}")
                    print(f"📊 Flight details: {seats} seats, {origin} → {destination}")
                    print("⚠️  Please save this ID for making reservations.")
                wait_for_user()
            except ValueError:
                print("\n❌ Invalid input. Please enter numeric values for days, hours, and seats.")
                wait_for_user()
                
        elif choice == '2':
            flight_id_str = input("\nEnter flight ID: ")
            try:
                flight_id = uuid.UUID(flight_id_str)
                status = reservation_system.get_flight_status(flight_id)
                if status:
                    print(f"\n✈️  FLIGHT: {status['origin']} → {status['destination']}")
                    print(f"    Total seats: {status['total_seats']}")
                    print(f"    Available seats: {status['available_seats']}")
                    print(f"    Reservations: {status['actual_reservations']}")
                    print(f"    Occupancy: {(status['actual_reservations']/status['total_seats']*100):.1f}%")
                else:
                    print("❌ Flight not found.")
                wait_for_user()
            except ValueError:
                print("❌ Invalid flight ID format. Please try again.")
                wait_for_user()
                
        elif choice == '3':
            return
        
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def reservation_menu(reservation_system):
    while True:
        display_header("🎫 RESERVATION MANAGEMENT")
        print("1. Make New Reservation")
        print("2. View Reservation Details")
        print("3. Update Reservation")
        print("4. Back to Main Menu")
        print("-"*50)
        
        choice = input("Enter your choice (1-4): ")
        
        if choice == '1':
            user_id_str = input("\nEnter user ID: ")
            flight_id_str = input("Enter flight ID: ")
            seat = input("Enter preferred seat (or leave empty for random seat): ").strip()
            seat = seat if seat else None
            
            try:
                user_id = uuid.UUID(user_id_str)
                flight_id = uuid.UUID(flight_id_str)
                reservation_id = reservation_system.make_reservation(user_id, flight_id, seat)
                wait_for_user()
            except ValueError:
                print("❌ Invalid ID format. Please try again.")
                wait_for_user()
                
        elif choice == '2':
            reservation_id_str = input("\nEnter reservation ID: ")
            try:
                reservation_id = uuid.UUID(reservation_id_str)
                reservation_system.get_reservation_details(reservation_id)
                wait_for_user()
            except ValueError:
                print("❌ Invalid reservation ID format. Please try again.")
                wait_for_user()
                
        elif choice == '3':
            reservation_id_str = input("\nEnter reservation ID: ")
            
            try:
                reservation_id = uuid.UUID(reservation_id_str)
                new_seat = input("Enter new seat (or leave empty to keep current): ").strip()
                new_seat = new_seat if new_seat else None
                
                status_options = ["confirmed", "cancelled", "waiting"]
                print("\nStatus options:")
                print("1 = Confirmed")
                print("2 = Cancelled")
                print("3 = Waiting")
                status_choice = input("\nEnter new status number (or leave empty to keep current): ").strip()
                
                new_status = None
                if status_choice and status_choice.isdigit():
                    idx = int(status_choice) - 1
                    if 0 <= idx < len(status_options):
                        new_status = status_options[idx]
                
                if new_seat or new_status:
                    reservation_system.update_reservation(reservation_id, new_seat, new_status)
                else:
                    print("\nNo changes specified.")
                wait_for_user()
                    
            except ValueError:
                print("❌ Invalid reservation ID format. Please try again.")
                wait_for_user()
                
        elif choice == '4':
            return
        
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def stress_test_menu(reservation_system):
    while True:
        display_header("🔥 STRESS TESTING")
        print("1. Run All Stress Tests")
        print("2. Run Rapid Identical Requests Test")
        print("3. Run Multiple Clients Test") 
        print("4. Run Fair Seat Competition Test")
        print("5. Back to Main Menu")
        print("-"*50)
        
        choice = input("Enter your choice (1-5): ")
        
        if choice == '1':
            print("\nGenerating test data and running all stress tests...")
            print("This will demonstrate all assignment requirements...")
            reservation_system.run_complete_stress_tests()
            wait_for_user()
                
        elif choice == '2':
            print("\nPreparing rapid identical requests test...")
            user_ids, flight_ids = reservation_system.generate_test_data(10, 2)  # Reduced from 30 to 10
            
            try:
                num = int(input("\nNumber of rapid requests to make (5-15): ") or "8")  # Reduced from 20 to 8
                reservation_system.stress_test_1_rapid_requests(user_ids, flight_ids, num_requests=num)
            except ValueError:
                print("❌ Invalid input. Using default of 8 requests.")
                reservation_system.stress_test_1_rapid_requests(user_ids, flight_ids, num_requests=8)
            wait_for_user()
            
        elif choice == '3':
            print("\nPreparing multiple clients test...")
            user_ids, flight_ids = reservation_system.generate_test_data(10, 2)  # Reduced from 30 to 10
            
            try:
                clients = int(input("\nNumber of concurrent clients (2-4): ") or "3")  # Reduced max from 10 to 4
                requests = int(input("Requests per client (3-8): ") or "5")  # Reduced from 10 to 5
                reservation_system.stress_test_2_multiple_clients(user_ids, flight_ids, 
                                                               num_clients=clients, 
                                                               requests_per_client=requests)
            except ValueError:
                print("❌ Invalid input. Using defaults of 3 clients and 5 requests each.")
                reservation_system.stress_test_2_multiple_clients(user_ids, flight_ids, 
                                                               num_clients=3, 
                                                               requests_per_client=5)
            wait_for_user()
            
        elif choice == '4':
            print("\nPreparing fair seat competition test...")
            user_ids, flight_ids = reservation_system.generate_test_data(8, 2)  # Reduced to 8 users
            
            print("This test demonstrates that both clients get fair access to reservations.")
            print("Assignment requirement: No single client monopolizes all seats.")
            
            try:
                clients = int(input("\nNumber of competing clients (2-3): ") or "2")  # Default to 2 for assignment
                reservation_system.stress_test_3_seat_competition(user_ids, flight_ids, num_clients=clients)
            except ValueError:
                print("❌ Invalid input. Using default of 2 clients.")
                reservation_system.stress_test_3_seat_competition(user_ids, flight_ids, num_clients=2)
            wait_for_user()
            
        elif choice == '5':
            return
        
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def airport_worker_menu(reservation_system):
    """access to all system data"""
    while True:
        display_header("👨‍✈️ AIRPORT WORKER VIEW")
        print("1. View All Users")
        print("2. View All Flights")
        print("3. View All Reservations")
        print("4. Back to Main Menu")
        print("-"*50)
        
        choice = input("Enter your choice (1-4): ")
        
        if choice == '1':
            page_size = int(input("\nNumber of users per page (5-50): ") or "10")
            page = 1
            while True:
                users = reservation_system.get_all_users(page, page_size)
                if not users:
                    print("\n❌ No users found or end of list reached.")
                    break
                    
                display_header(f"ALL USERS (Page {page})")
                for i, user in enumerate(users, 1):
                    print(f"{i}. ID: {user.user_id}")
                    print(f"   Username: {user.username}")
                    print(f"   Email: {user.email}")
                    print(f"   Created: {user.created_at}")
                    print()
                    
                nav = input("\nNext page (n), Previous page (p), Back to menu (b): ").lower()
                if nav == 'n':
                    page += 1
                elif nav == 'p' and page > 1:
                    page -= 1
                else:
                    break
            wait_for_user()
                
        elif choice == '2':
            page_size = int(input("\nNumber of flights per page (5-50): ") or "10")
            page = 1
            while True:
                flights = reservation_system.get_all_flights(page, page_size)
                if not flights:
                    print("\n❌ No flights found or end of list reached.")
                    break
                    
                display_header(f"ALL FLIGHTS (Page {page})")
                for i, flight in enumerate(flights, 1):
                    print(f"{i}. ID: {flight.flight_id}")
                    print(f"   Route: {flight.origin} → {flight.destination}")
                    print(f"   Departure: {flight.departure_time}")
                    print(f"   Available Seats: {flight.available_seats}")
                    print()
                    
                nav = input("\nNext page (n), Previous page (p), Back to menu (b): ").lower()
                if nav == 'n':
                    page += 1
                elif nav == 'p' and page > 1:
                    page -= 1
                else:
                    break
            wait_for_user()
                
        elif choice == '3':
            page_size = int(input("\nNumber of reservations per page (5-50): ") or "10")
            page = 1
            while True:
                reservations = reservation_system.get_all_reservations(page, page_size)
                if not reservations:
                    print("\n❌ No reservations found or end of list reached.")
                    break
                    
                display_header(f"ALL RESERVATIONS (Page {page})")
                for i, res in enumerate(reservations, 1):
                    print(f"{i}. ID: {res.reservation_id}")
                    print(f"   Flight: {res.flight_id}")
                    print(f"   User: {res.user_id}")
                    print(f"   Seat: {res.seat_number}")
                    print(f"   Status: {res.status}")
                    print(f"   Time: {res.reservation_time}")
                    print()
                    
                nav = input("\nNext page (n), Previous page (p), Back to menu (b): ").lower()
                if nav == 'n':
                    page += 1
                elif nav == 'p' and page > 1:
                    page -= 1
                else:
                    break
            wait_for_user()
                
        elif choice == '4':
            return
        
        else:
            print("\n❌ Invalid choice. Please try again.")
            wait_for_user()

def run_interactive_system():
    display_header("WELCOME TO THE FLIGHT RESERVATION SYSTEM")
    
    # system instance
    reservation_system = CassandraReservationSystem()
    
    print("\nConnecting to database...")
    if not reservation_system.initialize():
        print("❌ Failed to initialize reservation system")
        print("Please check if your Cassandra database is running.")
        return
    
    print("\n✅ Connected to database successfully!")
    print("Starting interactive menu...")
    wait_for_user()
    
    try:
        main_menu(reservation_system)
    except KeyboardInterrupt:
        print("\n\nExiting due to user interrupt.")
    finally:
        print("\nClosing database connections...")
        reservation_system.close()
        print("Thank you for using the Flight Reservation System!")

if __name__ == "__main__":
    run_interactive_system()